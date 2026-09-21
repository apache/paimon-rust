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

//! SQL context integration tests for paimon-datafusion.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::MemTable;
use paimon::catalog::{list_partitions_from_file_system, Identifier};
use paimon::spec::{
    ArrayType, BinaryType, BlobType, CharType, DataType, FloatType, IntType,
    LocalZonedTimestampType, MapType, MultisetType, SchemaChange, TimeType, VarBinaryType,
    VarCharType, VectorType,
};
use paimon::table::{BranchManager, SnapshotManager, TagManager};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonSchemaProvider, SQLContext};
use tempfile::TempDir;
use tokio::sync::Notify;

fn create_test_env() -> (TempDir, Arc<FileSystemCatalog>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    (temp_dir, Arc::new(catalog))
}

async fn create_sql_context(catalog: Arc<FileSystemCatalog>) -> SQLContext {
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();
    ctx
}

struct MetadataListingCatalog {
    get_table_calls: AtomicUsize,
    metadata_calls: AtomicUsize,
    reject_remote_calls: AtomicBool,
    fail_list_tables: AtomicBool,
    fail_list_tables_database: Mutex<Option<String>>,
    database_names: Mutex<Vec<String>>,
    list_tables_by_database: Mutex<std::collections::HashMap<String, usize>>,
    race_refreshes: AtomicBool,
    racing_list_tables_calls: AtomicUsize,
    stale_refresh_started: Notify,
    release_stale_refresh: Notify,
    require_parallel_object_listing: AtomicBool,
    view_listing_started: Notify,
    require_parallel_database_listing: AtomicBool,
    database_listings_started: AtomicUsize,
    parallel_database_listing_started: Notify,
    block_next_list_tables: AtomicBool,
    block_list_tables_database: Mutex<Option<String>>,
    blocked_list_tables_started: Notify,
    release_blocked_list_tables: Notify,
    table_names: Mutex<Vec<String>>,
    table_names_by_database: Mutex<std::collections::HashMap<String, Vec<String>>>,
    list_table_types_calls: AtomicUsize,
    fail_next_list_table_types: AtomicBool,
    listing_concurrency: Option<Arc<MetadataListingConcurrency>>,
    block_drop_response: AtomicBool,
    drop_committed: Notify,
    release_drop_response: Notify,
    rename_source_missing: AtomicBool,
    rename_ignore_flags: Mutex<Vec<bool>>,
}

#[derive(Default)]
struct MetadataListingConcurrency {
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl MetadataListingConcurrency {
    fn reset(&self) {
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
        self.maximum.store(0, Ordering::SeqCst);
    }

    fn maximum(&self) -> usize {
        self.maximum.load(Ordering::SeqCst)
    }

    async fn observe(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MetadataListingCatalog {
    fn new() -> Self {
        Self {
            get_table_calls: AtomicUsize::new(0),
            metadata_calls: AtomicUsize::new(0),
            reject_remote_calls: AtomicBool::new(false),
            fail_list_tables: AtomicBool::new(false),
            fail_list_tables_database: Mutex::new(None),
            database_names: Mutex::new(vec!["default".to_string()]),
            list_tables_by_database: Mutex::new(std::collections::HashMap::new()),
            race_refreshes: AtomicBool::new(false),
            racing_list_tables_calls: AtomicUsize::new(0),
            stale_refresh_started: Notify::new(),
            release_stale_refresh: Notify::new(),
            require_parallel_object_listing: AtomicBool::new(false),
            view_listing_started: Notify::new(),
            require_parallel_database_listing: AtomicBool::new(false),
            database_listings_started: AtomicUsize::new(0),
            parallel_database_listing_started: Notify::new(),
            block_next_list_tables: AtomicBool::new(false),
            block_list_tables_database: Mutex::new(None),
            blocked_list_tables_started: Notify::new(),
            release_blocked_list_tables: Notify::new(),
            table_names: Mutex::new(vec!["metadata_only".to_string()]),
            table_names_by_database: Mutex::new(std::collections::HashMap::new()),
            list_table_types_calls: AtomicUsize::new(0),
            fail_next_list_table_types: AtomicBool::new(false),
            listing_concurrency: None,
            block_drop_response: AtomicBool::new(false),
            drop_committed: Notify::new(),
            release_drop_response: Notify::new(),
            rename_source_missing: AtomicBool::new(false),
            rename_ignore_flags: Mutex::new(Vec::new()),
        }
    }

    fn with_databases(databases: Vec<&str>) -> Self {
        let catalog = Self::new();
        *catalog.database_names.lock().unwrap() =
            databases.into_iter().map(ToString::to_string).collect();
        catalog
    }

    fn with_listing_concurrency(listing_concurrency: Arc<MetadataListingConcurrency>) -> Self {
        Self {
            listing_concurrency: Some(listing_concurrency),
            ..Self::new()
        }
    }

    fn set_databases(&self, databases: Vec<&str>) {
        *self.database_names.lock().unwrap() =
            databases.into_iter().map(ToString::to_string).collect();
    }

    fn get_table_calls(&self) -> usize {
        self.get_table_calls.load(Ordering::SeqCst)
    }

    fn metadata_calls(&self) -> usize {
        self.metadata_calls.load(Ordering::SeqCst)
    }

    fn reject_remote_calls(&self) {
        self.reject_remote_calls.store(true, Ordering::SeqCst);
    }

    fn set_table_names(&self, names: Vec<&str>) {
        *self.table_names.lock().unwrap() = names.into_iter().map(ToString::to_string).collect();
    }

    fn set_table_names_for(&self, database: &str, names: Vec<&str>) {
        self.table_names_by_database.lock().unwrap().insert(
            database.to_string(),
            names.into_iter().map(ToString::to_string).collect(),
        );
    }

    fn fail_list_tables(&self) {
        self.fail_list_tables.store(true, Ordering::SeqCst);
    }

    fn fail_list_tables_for(&self, database: &str) {
        *self.fail_list_tables_database.lock().unwrap() = Some(database.to_string());
    }

    fn list_tables_calls_for(&self, database: &str) -> usize {
        self.list_tables_by_database
            .lock()
            .unwrap()
            .get(database)
            .copied()
            .unwrap_or_default()
    }

    fn list_table_types_calls(&self) -> usize {
        self.list_table_types_calls.load(Ordering::SeqCst)
    }

    fn fail_next_list_table_types(&self) {
        self.fail_next_list_table_types
            .store(true, Ordering::SeqCst);
    }

    fn race_next_refreshes(&self) {
        self.racing_list_tables_calls.store(0, Ordering::SeqCst);
        self.race_refreshes.store(true, Ordering::SeqCst);
    }

    fn require_parallel_object_listing(&self) {
        self.require_parallel_object_listing
            .store(true, Ordering::SeqCst);
    }

    fn require_parallel_database_listing(&self) {
        self.require_parallel_database_listing
            .store(true, Ordering::SeqCst);
    }

    fn block_next_list_tables(&self) {
        self.block_next_list_tables.store(true, Ordering::SeqCst);
    }

    fn block_next_list_tables_for(&self, database: &str) {
        *self.block_list_tables_database.lock().unwrap() = Some(database.to_string());
    }

    fn block_next_drop_response(&self) {
        self.block_drop_response.store(true, Ordering::SeqCst);
    }

    fn mark_rename_source_missing(&self) {
        self.rename_source_missing.store(true, Ordering::SeqCst);
    }

    fn rename_ignore_flags(&self) -> Vec<bool> {
        self.rename_ignore_flags.lock().unwrap().clone()
    }

    fn record_remote_call(&self) {
        assert!(
            !self.reject_remote_calls.load(Ordering::SeqCst),
            "synchronous provider callback accessed the remote catalog"
        );
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Catalog for MetadataListingCatalog {
    async fn list_databases(&self) -> paimon::Result<Vec<String>> {
        self.record_remote_call();
        Ok(self.database_names.lock().unwrap().clone())
    }

    async fn create_database(
        &self,
        _name: &str,
        _ignore_if_exists: bool,
        _properties: std::collections::HashMap<String, String>,
    ) -> paimon::Result<()> {
        Ok(())
    }

    async fn get_database(&self, name: &str) -> paimon::Result<paimon::catalog::Database> {
        self.record_remote_call();
        Ok(paimon::catalog::Database::new(
            name.to_string(),
            std::collections::HashMap::new(),
            None,
        ))
    }

    async fn drop_database(
        &self,
        _name: &str,
        _ignore_if_not_exists: bool,
        _cascade: bool,
    ) -> paimon::Result<()> {
        if self.block_drop_response.swap(false, Ordering::SeqCst) {
            self.drop_committed.notify_one();
            self.release_drop_response.notified().await;
        }
        Ok(())
    }

    async fn get_table(&self, _identifier: &Identifier) -> paimon::Result<paimon::table::Table> {
        self.record_remote_call();
        self.get_table_calls.fetch_add(1, Ordering::SeqCst);
        Err(paimon::Error::Unsupported {
            message: "table loading is unavailable".to_string(),
        })
    }

    async fn list_tables(&self, database_name: &str) -> paimon::Result<Vec<String>> {
        self.record_remote_call();
        if let Some(listing_concurrency) = &self.listing_concurrency {
            listing_concurrency.observe().await;
        }
        *self
            .list_tables_by_database
            .lock()
            .unwrap()
            .entry(database_name.to_string())
            .or_default() += 1;
        if self.fail_list_tables.load(Ordering::SeqCst)
            || self.fail_list_tables_database.lock().unwrap().as_deref() == Some(database_name)
        {
            return Err(paimon::Error::Unsupported {
                message: "simulated metadata refresh failure".to_string(),
            });
        }
        if self.race_refreshes.load(Ordering::SeqCst) {
            let call = self.racing_list_tables_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.stale_refresh_started.notify_one();
                self.release_stale_refresh.notified().await;
                return Ok(vec!["stale".to_string()]);
            }
            return Ok(vec!["fresh".to_string()]);
        }
        if self.require_parallel_object_listing.load(Ordering::SeqCst) {
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                self.view_listing_started.notified(),
            )
            .await
            .map_err(|_| paimon::Error::Unsupported {
                message: "list_views did not run concurrently".to_string(),
            })?;
        }
        if self
            .require_parallel_database_listing
            .load(Ordering::SeqCst)
        {
            let started = self
                .database_listings_started
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            if started == 1 {
                tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    self.parallel_database_listing_started.notified(),
                )
                .await
                .map_err(|_| paimon::Error::Unsupported {
                    message: "databases were not listed concurrently".to_string(),
                })?;
            } else {
                self.parallel_database_listing_started.notify_waiters();
            }
        }
        let table_names = self
            .table_names_by_database
            .lock()
            .unwrap()
            .get(database_name)
            .cloned()
            .unwrap_or_else(|| self.table_names.lock().unwrap().clone());
        let block_target = {
            let mut block_database = self.block_list_tables_database.lock().unwrap();
            if block_database.as_deref() == Some(database_name) {
                block_database.take();
                true
            } else {
                false
            }
        };
        let should_block =
            block_target || self.block_next_list_tables.swap(false, Ordering::SeqCst);
        if should_block {
            self.blocked_list_tables_started.notify_one();
            self.release_blocked_list_tables.notified().await;
        }
        Ok(table_names)
    }

    async fn list_table_types(
        &self,
        _database_name: &str,
        table_names: &[String],
    ) -> paimon::Result<std::collections::HashMap<String, paimon::spec::TableType>> {
        self.list_table_types_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(listing_concurrency) = &self.listing_concurrency {
            listing_concurrency.observe().await;
        }
        if self
            .fail_next_list_table_types
            .swap(false, Ordering::SeqCst)
        {
            return Err(paimon::Error::Unsupported {
                message: "simulated table type classification failure".to_string(),
            });
        }
        Ok(table_names
            .iter()
            .cloned()
            .map(|name| (name, paimon::spec::TableType::Table))
            .collect())
    }

    async fn list_views(&self, _database_name: &str) -> paimon::Result<Vec<String>> {
        self.record_remote_call();
        if self.require_parallel_object_listing.load(Ordering::SeqCst) {
            self.view_listing_started.notify_one();
        }
        Ok(vec!["metadata_view".to_string()])
    }

    async fn create_table(
        &self,
        _identifier: &Identifier,
        _creation: paimon::spec::Schema,
        _ignore_if_exists: bool,
    ) -> paimon::Result<()> {
        Ok(())
    }

    async fn drop_table(
        &self,
        _identifier: &Identifier,
        _ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        if self.block_drop_response.swap(false, Ordering::SeqCst) {
            self.drop_committed.notify_one();
            self.release_drop_response.notified().await;
        }
        Ok(())
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.rename_ignore_flags
            .lock()
            .unwrap()
            .push(ignore_if_not_exists);
        if self.rename_source_missing.load(Ordering::SeqCst) {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(paimon::Error::TableNotExist {
                full_name: from.full_name(),
            });
        }
        let mut table_names_by_database = self.table_names_by_database.lock().unwrap();
        if let Some(names) = table_names_by_database.get_mut(from.database()) {
            if let Some(index) = names.iter().position(|name| name == from.object()) {
                names[index] = to.object().to_string();
            }
            return Ok(());
        }
        drop(table_names_by_database);
        let mut names = self.table_names.lock().unwrap();
        if let Some(index) = names.iter().position(|name| name == from.object()) {
            names[index] = to.object().to_string();
        }
        Ok(())
    }

    async fn alter_table(
        &self,
        _identifier: &Identifier,
        _changes: Vec<SchemaChange>,
        _ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_refreshed_catalog_callbacks_do_not_access_remote_catalog() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = PaimonCatalogProvider::new_uninitialized(
        Some("paimon".to_string()),
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    );

    provider.refresh_metadata().await.unwrap();
    let calls_after_refresh = catalog.metadata_calls();
    catalog.reject_remote_calls();

    assert_eq!(provider.schema_names(), vec!["default"]);
    let schema = provider.schema("default").unwrap();
    assert_eq!(schema.table_names(), vec!["metadata_only", "metadata_view"]);
    assert!(schema.table_exist("metadata_only"));
    assert!(schema.table_exist("metadata_view"));
    assert!(!schema.table_exist("missing"));
    assert_eq!(catalog.metadata_calls(), calls_after_refresh);
}

#[tokio::test]
async fn test_public_catalog_constructor_returns_ready_provider() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog,
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(provider.schema_names(), vec!["default"]);
    assert_eq!(
        provider.schema("default").unwrap().table_names(),
        vec!["metadata_only", "metadata_view"]
    );
}

#[tokio::test]
async fn test_metadata_refresh_classifies_only_new_objects() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec!["existing"]);
    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(catalog.list_table_types_calls(), 1);

    provider.refresh_metadata().await.unwrap();
    assert_eq!(catalog.list_table_types_calls(), 1);

    catalog.set_table_names(vec!["existing", "discovered"]);
    provider.refresh_metadata().await.unwrap();
    assert_eq!(catalog.list_table_types_calls(), 2);
    provider.refresh_metadata().await.unwrap();
    assert_eq!(catalog.list_table_types_calls(), 2);
}

#[tokio::test]
async fn test_metadata_refresh_retries_unknown_table_types() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec!["existing"]);
    catalog.fail_next_list_table_types();
    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();
    let schema = provider.schema("default").unwrap();
    assert!(!schema.table_exist("existing$snapshots"));
    assert_eq!(catalog.list_table_types_calls(), 1);

    provider.refresh_metadata().await.unwrap();

    assert_eq!(catalog.list_table_types_calls(), 2);
    let schema = provider.schema("default").unwrap();
    assert!(schema.table_exist("existing$snapshots"));
}

#[tokio::test]
async fn test_explicit_uninitialized_providers_require_metadata_initialization() {
    let catalog: Arc<dyn Catalog> = Arc::new(MetadataListingCatalog::new());
    let catalog_provider = PaimonCatalogProvider::new_uninitialized(
        Some("paimon".to_string()),
        Arc::clone(&catalog),
        Default::default(),
        Default::default(),
        None,
    );
    assert!(catalog_provider.schema_names().is_empty());
    catalog_provider.initialize_metadata().await.unwrap();
    assert_eq!(catalog_provider.schema_names(), vec!["default"]);

    let schema_provider = PaimonSchemaProvider::new_uninitialized(
        Some("paimon".to_string()),
        catalog,
        "default".to_string(),
        Default::default(),
        None,
        Default::default(),
        None,
    );
    assert!(schema_provider.table_names().is_empty());
    schema_provider.initialize_metadata().await.unwrap();
    assert_eq!(
        schema_provider.table_names(),
        vec!["metadata_only", "metadata_view"]
    );
}

#[tokio::test]
async fn test_failed_catalog_refresh_preserves_last_good_snapshot() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();

    catalog.set_table_names(vec!["not_committed"]);
    catalog.fail_list_tables();
    assert!(provider.refresh_metadata().await.is_err());

    assert_eq!(provider.schema_names(), vec!["default"]);
    let schema = provider.schema("default").unwrap();
    assert_eq!(schema.table_names(), vec!["metadata_only", "metadata_view"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_older_refresh_cannot_overwrite_newer_snapshot() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = Arc::new(
        PaimonCatalogProvider::try_new(
            Some("paimon".to_string()),
            catalog.clone(),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .unwrap(),
    );
    catalog.race_next_refreshes();

    let stale_provider = Arc::clone(&provider);
    let stale_refresh = tokio::spawn(async move { stale_provider.refresh_metadata().await });
    catalog.stale_refresh_started.notified().await;

    let fresh_provider = Arc::clone(&provider);
    tokio::spawn(async move { fresh_provider.refresh_metadata().await })
        .await
        .unwrap()
        .unwrap();

    catalog.release_stale_refresh.notify_one();
    stale_refresh.await.unwrap().unwrap();

    let schema = provider.schema("default").unwrap();
    assert_eq!(schema.table_names(), vec!["fresh", "metadata_view"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_refresh_merges_non_conflicting_database_updates() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "first", "second",
    ]));
    catalog.set_table_names_for("first", vec!["first_old"]);
    catalog.set_table_names_for("second", vec!["second_old"]);
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.set_table_names_for("first", vec!["first_new"]);
    catalog.set_table_names_for("second", vec!["second_stale"]);
    catalog.block_next_list_tables_for("second");
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let full_refresh = tokio::spawn(async move {
        provider
            .downcast_ref::<PaimonCatalogProvider>()
            .unwrap()
            .refresh_metadata()
            .await
    });
    catalog.blocked_list_tables_started.notified().await;

    catalog.set_table_names_for("second", vec!["second_new"]);
    assert!(sql_context
        .sql("SELECT * FROM paimon.second.second_new")
        .await
        .is_err());
    catalog.release_blocked_list_tables.notify_one();
    full_refresh.await.unwrap().unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert_eq!(
        provider.schema("first").unwrap().table_names(),
        vec!["first_new", "metadata_view"]
    );
    assert_eq!(
        provider.schema("second").unwrap().table_names(),
        vec!["second_new", "metadata_view"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_refresh_retries_database_rejected_by_ddl_delta() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec!["existing"]);
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.set_table_names(vec!["existing", "discovered"]);
    catalog.block_next_list_tables_for("default");
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let full_refresh = tokio::spawn(async move {
        provider
            .downcast_ref::<PaimonCatalogProvider>()
            .unwrap()
            .refresh_metadata()
            .await
    });
    catalog.blocked_list_tables_started.notified().await;

    sql_context
        .sql("CREATE TABLE created (id BIGINT)")
        .await
        .unwrap();
    catalog.set_table_names(vec!["existing", "discovered", "created"]);
    catalog.release_blocked_list_tables.notify_one();
    full_refresh.await.unwrap().unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("default").unwrap();
    assert!(schema.table_exist("created"));
    assert!(schema.table_exist("discovered"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_newer_full_refresh_advances_database_tombstone() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "first", "second",
    ]));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    sql_context.sql("DROP DATABASE second").await.unwrap();

    catalog.block_next_list_tables_for("second");
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let stale_refresh = tokio::spawn(async move {
        provider
            .downcast_ref::<PaimonCatalogProvider>()
            .unwrap()
            .refresh_metadata()
            .await
    });
    catalog.blocked_list_tables_started.notified().await;

    catalog.set_databases(vec!["first"]);
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    provider
        .downcast_ref::<PaimonCatalogProvider>()
        .unwrap()
        .refresh_metadata()
        .await
        .unwrap();
    catalog.release_blocked_list_tables.notify_one();
    stale_refresh.await.unwrap().unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert!(provider.schema("second").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stale_refresh_cannot_overwrite_local_schema_registration() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = Arc::new(
        PaimonCatalogProvider::try_new(
            Some("paimon".to_string()),
            catalog.clone(),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .unwrap(),
    );
    catalog.race_next_refreshes();

    let stale_provider = Arc::clone(&provider);
    let stale_refresh = tokio::spawn(async move { stale_provider.refresh_metadata().await });
    catalog.stale_refresh_started.notified().await;

    provider
        .register_schema(
            "local",
            Arc::new(datafusion::catalog::MemorySchemaProvider::new()),
        )
        .unwrap();
    assert!(provider.schema("local").is_some());

    catalog.release_stale_refresh.notify_one();
    stale_refresh.await.unwrap().unwrap();

    assert!(provider.schema("local").is_some());
}

#[tokio::test]
async fn test_refresh_lists_tables_and_views_concurrently() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.require_parallel_object_listing();

    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog,
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        provider.schema("default").unwrap().table_names(),
        vec!["metadata_only", "metadata_view"]
    );
}

#[tokio::test]
async fn test_refresh_lists_databases_concurrently() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "first", "second",
    ]));
    catalog.require_parallel_database_listing();

    let provider = PaimonCatalogProvider::try_new(
        Some("paimon".to_string()),
        catalog,
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();

    assert_eq!(provider.schema_names(), vec!["first", "second"]);
}

#[tokio::test]
async fn test_temp_table_registration_does_not_access_remote_catalog() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let provider = PaimonCatalogProvider::new_uninitialized(
        Some("paimon".to_string()),
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    );
    provider.refresh_metadata().await.unwrap();
    let calls_after_refresh = catalog.metadata_calls();
    catalog.reject_remote_calls();

    let table = MemTable::try_new(Arc::new(Schema::empty()), vec![vec![]]).unwrap();
    provider
        .register_temp_table("default", "metadata_only", Arc::new(table))
        .unwrap();

    assert_eq!(catalog.metadata_calls(), calls_after_refresh);
}

struct PartitionCatalog {
    inner: Arc<FileSystemCatalog>,
    fail_list_partitions: AtomicBool,
    unknown_statistics: AtomicBool,
    partition_identifiers: Mutex<Vec<Identifier>>,
}

impl PartitionCatalog {
    fn new(inner: Arc<FileSystemCatalog>) -> Self {
        Self {
            inner,
            fail_list_partitions: AtomicBool::new(false),
            unknown_statistics: AtomicBool::new(false),
            partition_identifiers: Mutex::new(Vec::new()),
        }
    }

    fn set_fail_list_partitions(&self, fail: bool) {
        self.fail_list_partitions.store(fail, Ordering::SeqCst);
    }

    /// Report every statistic as never measured, the way a catalog does for a partition that was
    /// registered but never had statistics reported to it.
    fn set_unknown_statistics(&self, unknown: bool) {
        self.unknown_statistics.store(unknown, Ordering::SeqCst);
    }

    fn take_partition_identifiers(&self) -> Vec<Identifier> {
        std::mem::take(&mut *self.partition_identifiers.lock().unwrap())
    }
}

#[async_trait]
impl Catalog for PartitionCatalog {
    async fn list_databases(&self) -> paimon::Result<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: std::collections::HashMap<String, String>,
    ) -> paimon::Result<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> paimon::Result<paimon::catalog::Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> paimon::Result<paimon::table::Table> {
        self.inner.get_table(identifier).await
    }

    async fn list_tables(&self, database_name: &str) -> paimon::Result<Vec<String>> {
        self.inner.list_tables(database_name).await
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: paimon::spec::Schema,
        ignore_if_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }

    async fn list_partitions(
        &self,
        identifier: &Identifier,
    ) -> paimon::Result<Vec<paimon::spec::Partition>> {
        self.partition_identifiers
            .lock()
            .unwrap()
            .push(identifier.clone());

        let Some(branch) = identifier.branch_name()? else {
            let mut partitions = self.inner.list_partitions(identifier).await?;
            if self.unknown_statistics.load(Ordering::SeqCst) {
                for partition in &mut partitions {
                    partition.record_count = paimon::spec::Partition::UNKNOWN;
                    partition.file_size_in_bytes = paimon::spec::Partition::UNKNOWN;
                    partition.file_count = paimon::spec::Partition::UNKNOWN;
                    partition.last_file_creation_time = paimon::spec::Partition::UNKNOWN;
                    partition.total_buckets = paimon::spec::Partition::UNKNOWN_TOTAL_BUCKETS;
                }
            }
            return Ok(partitions);
        };
        if self.fail_list_partitions.load(Ordering::SeqCst) {
            return Err(paimon::Error::Unsupported {
                message: "injected list_partitions failure".to_string(),
            });
        }

        let base = Identifier::new(identifier.database(), identifier.table_name()?);
        let table = self.inner.get_table(&base).await?;
        let table = table.copy_with_branch(&branch).await?;
        let mut partitions = list_partitions_from_file_system(&table).await?;
        for partition in &mut partitions {
            partition.created_by = Some("catalog".to_string());
        }
        Ok(partitions)
    }
}

async fn collect_ids(sql_context: &SQLContext, sql: &str) -> Vec<i32> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        for row in 0..batch.num_rows() {
            ids.push(id_array.value(row));
        }
    }
    ids.sort_unstable();
    ids
}

async fn collect_i64_column(sql_context: &SQLContext, sql: &str, column: &str) -> Vec<i64> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect(column);
        for row in 0..batch.num_rows() {
            values.push(array.value(row));
        }
    }
    values.sort_unstable();
    values
}

async fn collect_string_column(sql_context: &SQLContext, sql: &str, column: &str) -> Vec<String> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect(column);
        for row in 0..batch.num_rows() {
            if !array.is_null(row) {
                values.push(array.value(row).to_string());
            }
        }
    }
    values.sort_unstable();
    values
}

async fn assert_sql_error_contains(sql_context: &SQLContext, sql: &str, expected: &str) {
    let err = match sql_context.sql(sql).await {
        Ok(df) => df
            .collect()
            .await
            .expect_err("SQL should fail but succeeded")
            .to_string(),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains(expected),
        "expected error containing '{expected}', got: {err}"
    );
}

#[tokio::test]
async fn test_show_tables_is_enabled() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("SHOW TABLES")
        .await
        .expect("SHOW TABLES should be planned when information_schema is enabled")
        .collect()
        .await
        .expect("SHOW TABLES should execute");
}

#[tokio::test]
async fn test_show_tables_does_not_load_table_providers() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    let table_names = collect_string_column(&sql_context, "SHOW TABLES", "table_name").await;

    assert!(table_names.contains(&"metadata_only".to_string()));
    assert_eq!(catalog.get_table_calls(), 0);

    assert!(sql_context
        .sql("SELECT * FROM metadata_only")
        .await
        .is_err());
    assert!(catalog.get_table_calls() > 0);
}

#[tokio::test]
async fn test_show_tables_refreshes_catalog_metadata() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.set_table_names(vec!["added_after_registration"]);
    let table_names = collect_string_column(&sql_context, "SHOW TABLES", "table_name").await;

    assert!(table_names.contains(&"added_after_registration".to_string()));
    assert!(!table_names.contains(&"metadata_only".to_string()));
}

#[tokio::test]
async fn test_show_tables_does_not_refresh_unrelated_catalogs() {
    let healthy = Arc::new(MetadataListingCatalog::new());
    let unavailable = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("healthy", healthy.clone())
        .await
        .unwrap();
    sql_context
        .register_catalog("unavailable", unavailable.clone())
        .await
        .unwrap();

    healthy.set_table_names(vec!["current"]);
    unavailable.fail_list_tables();
    let unavailable_calls = unavailable.metadata_calls();

    let table_names = collect_string_column(&sql_context, "SHOW TABLES", "table_name").await;

    assert!(table_names.contains(&"current".to_string()));
    assert_eq!(unavailable.metadata_calls(), unavailable_calls);
}

#[tokio::test]
async fn test_show_tables_only_refreshes_current_database() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "default",
        "unavailable",
    ]));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.set_table_names(vec!["current"]);
    catalog.fail_list_tables_for("unavailable");
    let unavailable_calls = catalog.list_tables_calls_for("unavailable");

    let table_names = collect_string_column(&sql_context, "SHOW TABLES", "table_name").await;

    assert!(table_names.contains(&"current".to_string()));
    assert_eq!(
        catalog.list_tables_calls_for("unavailable"),
        unavailable_calls
    );
}

#[tokio::test]
async fn test_show_columns_refreshes_qualified_database() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "default",
        "analytics",
    ]));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    let default_calls = catalog.list_tables_calls_for("default");
    let analytics_calls = catalog.list_tables_calls_for("analytics");
    assert!(sql_context
        .sql("SHOW COLUMNS IN paimon.analytics.metadata_only")
        .await
        .is_err());

    assert_eq!(catalog.list_tables_calls_for("default"), default_calls);
    assert_eq!(
        catalog.list_tables_calls_for("analytics"),
        analytics_calls + 1
    );
}

#[tokio::test]
async fn test_show_functions_does_not_refresh_catalog_metadata() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.fail_list_tables();
    let list_calls = catalog.list_tables_calls_for("default");
    sql_context.sql("SHOW FUNCTIONS").await.unwrap();

    assert_eq!(catalog.list_tables_calls_for("default"), list_calls);
}

#[tokio::test]
async fn test_create_table_does_not_refresh_unrelated_catalogs() {
    let healthy = Arc::new(MetadataListingCatalog::new());
    let unrelated = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("healthy", healthy)
        .await
        .unwrap();
    sql_context
        .register_catalog("unrelated", unrelated.clone())
        .await
        .unwrap();
    let unrelated_calls = unrelated.metadata_calls();

    sql_context
        .sql("CREATE TABLE created (id BIGINT)")
        .await
        .unwrap();

    assert_eq!(unrelated.metadata_calls(), unrelated_calls);
}

#[tokio::test]
async fn test_successful_ddl_does_not_wait_for_metadata_listing() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.block_next_list_tables();

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        sql_context.sql("CREATE TABLE direct_snapshot_update (id BIGINT)"),
    )
    .await
    .expect("committed DDL must not wait for a metadata listing")
    .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert!(provider
        .schema("default")
        .unwrap()
        .table_names()
        .contains(&"direct_snapshot_update".to_string()));
}

#[tokio::test]
async fn test_ddl_applies_exact_snapshot_deltas() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let default_list_calls = catalog.list_tables_calls_for("default");

    sql_context.sql("CREATE DATABASE analytics").await.unwrap();
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert!(provider.schema("analytics").is_some());

    sql_context
        .sql("CREATE TABLE analytics.events (id BIGINT)")
        .await
        .unwrap();
    assert!(provider.schema("analytics").unwrap().table_exist("events"));

    sql_context
        .sql("ALTER TABLE analytics.events RENAME TO renamed_events")
        .await
        .unwrap();
    let schema = provider.schema("analytics").unwrap();
    assert!(!schema.table_exist("events"));
    assert!(schema.table_exist("renamed_events"));

    sql_context
        .sql("DROP TABLE analytics.renamed_events")
        .await
        .unwrap();
    assert!(!provider
        .schema("analytics")
        .unwrap()
        .table_exist("renamed_events"));

    sql_context.sql("DROP DATABASE analytics").await.unwrap();
    assert!(provider.schema("analytics").is_none());
    assert_eq!(
        catalog.list_tables_calls_for("default"),
        default_list_calls,
        "DDL deltas must not trigger metadata listings"
    );
}

#[tokio::test]
async fn test_alter_table_if_exists_does_not_create_phantom_rename_delta() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog)
        .await
        .unwrap();

    sql_context
        .sql("ALTER TABLE IF EXISTS missing RENAME TO phantom")
        .await
        .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("default").unwrap();
    assert!(!schema.table_exist("missing"));
    assert!(!schema.table_exist("phantom"));
}

#[tokio::test]
async fn test_alter_table_if_exists_reconciles_stale_positive_rename_source() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec!["source"]);
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog.set_table_names(vec![]);
    catalog.mark_rename_source_missing();
    sql_context
        .sql("ALTER TABLE IF EXISTS source RENAME TO phantom")
        .await
        .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("default").unwrap();
    assert!(!schema.table_exist("source"));
    assert!(!schema.table_exist("phantom"));
    assert_eq!(catalog.rename_ignore_flags(), vec![false]);
}

#[tokio::test]
async fn test_successful_rename_refreshes_target_when_source_was_not_snapshotted() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec![]);
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.set_table_names(vec!["source"]);

    sql_context
        .sql("ALTER TABLE source RENAME TO destination")
        .await
        .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("default").unwrap();
    assert!(!schema.table_exist("source"));
    assert!(schema.table_exist("destination"));
    assert!(schema.table_exist("destination$snapshots"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_ddl_delta_follows_serialized_commit_order() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    catalog.set_table_names(vec!["raced"]);
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let sql_context = Arc::new(sql_context);
    catalog.block_next_drop_response();

    let drop_context = Arc::clone(&sql_context);
    let drop = tokio::spawn(async move { drop_context.sql("DROP TABLE raced").await });
    catalog.drop_committed.notified().await;

    let create_finished = Arc::new(Notify::new());
    let create_context = Arc::clone(&sql_context);
    let create_finished_signal = Arc::clone(&create_finished);
    let create = tokio::spawn(async move {
        let result = create_context.sql("CREATE TABLE raced (id BIGINT)").await;
        create_finished_signal.notify_one();
        result
    });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            create_finished.notified(),
        )
        .await
        .is_err(),
        "same-database DDL must wait for the earlier mutation"
    );
    catalog.release_drop_response.notify_one();

    drop.await.unwrap().unwrap();
    create.await.unwrap().unwrap();
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert!(provider.schema("default").unwrap().table_exist("raced"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_database_and_table_ddl_share_serialized_commit_order() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let sql_context = Arc::new(sql_context);
    catalog.block_next_drop_response();

    let drop_context = Arc::clone(&sql_context);
    let drop = tokio::spawn(async move { drop_context.sql("DROP DATABASE default").await });
    catalog.drop_committed.notified().await;

    let create_finished = Arc::new(Notify::new());
    let create_context = Arc::clone(&sql_context);
    let create_finished_signal = Arc::clone(&create_finished);
    let create = tokio::spawn(async move {
        let result = create_context
            .sql("CREATE TABLE default.after_drop (id BIGINT)")
            .await;
        create_finished_signal.notify_one();
        result
    });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            create_finished.notified(),
        )
        .await
        .is_err(),
        "same-database table DDL must wait for database DDL"
    );
    catalog.release_drop_response.notify_one();

    drop.await.unwrap().unwrap();
    create.await.unwrap().unwrap();
    let provider = sql_context.ctx().catalog("paimon").unwrap();
    assert!(provider
        .schema("default")
        .unwrap()
        .table_exist("after_drop"));
}

#[tokio::test]
async fn test_repeated_missing_tables_share_negative_refresh_ttl() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let list_calls = catalog.list_tables_calls_for("default");

    assert!(sql_context
        .sql("SELECT * FROM first_missing")
        .await
        .is_err());
    assert!(sql_context
        .sql("SELECT * FROM second_missing")
        .await
        .is_err());

    assert_eq!(catalog.list_tables_calls_for("default"), list_calls + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_missing_tables_share_single_refresh() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let sql_context = Arc::new(sql_context);
    let list_calls = catalog.list_tables_calls_for("default");
    catalog.block_next_list_tables();

    let first_context = Arc::clone(&sql_context);
    let first = tokio::spawn(async move { first_context.sql("SELECT * FROM missing_a").await });
    catalog.blocked_list_tables_started.notified().await;

    let second_context = Arc::clone(&sql_context);
    let second = tokio::spawn(async move { second_context.sql("SELECT * FROM missing_b").await });
    tokio::task::yield_now().await;
    catalog.release_blocked_list_tables.notify_one();

    assert!(first.await.unwrap().is_err());
    assert!(second.await.unwrap().is_err());
    assert_eq!(catalog.list_tables_calls_for("default"), list_calls + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stalled_missing_table_refresh_does_not_block_metadata_free_query() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let sql_context = Arc::new(sql_context);
    catalog.block_next_list_tables();

    let missing_context = Arc::clone(&sql_context);
    let missing = tokio::spawn(async move { missing_context.sql("SELECT * FROM missing").await });
    catalog.blocked_list_tables_started.notified().await;

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        sql_context.sql("SELECT 1"),
    )
    .await
    .expect("metadata-free query must not wait for an unrelated refresh")
    .unwrap();

    catalog.release_blocked_list_tables.notify_one();
    assert!(missing.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stalled_refresh_does_not_block_another_database() {
    let catalog = Arc::new(MetadataListingCatalog::with_databases(vec![
        "first", "second",
    ]));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let sql_context = Arc::new(sql_context);
    catalog.block_next_list_tables_for("first");

    let first_context = Arc::clone(&sql_context);
    let first = tokio::spawn(async move {
        first_context
            .sql("SELECT * FROM paimon.first.missing")
            .await
    });
    catalog.blocked_list_tables_started.notified().await;

    let second_result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        sql_context.sql("SELECT * FROM paimon.second.missing"),
    )
    .await
    .expect("a different database must not wait for the stalled refresh");
    assert!(second_result.is_err());

    catalog.release_blocked_list_tables.notify_one();
    assert!(first.await.unwrap().is_err());
}

#[tokio::test]
async fn test_show_tables_preserves_catalog_view_type() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    let table_types = collect_string_column(
        &sql_context,
        "SELECT table_type FROM information_schema.tables \
         WHERE table_schema = 'default' AND table_name = 'metadata_view'",
        "table_type",
    )
    .await;

    assert_eq!(table_types, vec!["VIEW"]);
    assert_eq!(catalog.get_table_calls(), 0);
}

#[tokio::test]
async fn test_information_schema_refreshes_all_paimon_catalogs() {
    let first = Arc::new(MetadataListingCatalog::new());
    let second = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .build();
    sql_context.register_catalog("first", first).await.unwrap();
    sql_context
        .register_catalog("second", second.clone())
        .await
        .unwrap();
    second.set_table_names(vec!["second_new"]);

    let table_names = collect_string_column(
        &sql_context,
        "SELECT table_name FROM first.information_schema.tables \
         WHERE table_catalog = 'second' AND table_schema = 'default' \
         AND table_name = 'second_new'",
        "table_name",
    )
    .await;

    assert_eq!(table_names, vec!["second_new"]);
}

#[tokio::test]
async fn test_information_schema_reuses_recent_catalog_refresh() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::from_millis(200))
        .build();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();
    let calls_after_first_query = catalog.metadata_calls();

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(catalog.metadata_calls(), calls_after_first_query);
}

#[tokio::test]
async fn test_information_schema_reuses_registration_snapshot() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    let calls_after_registration = catalog.metadata_calls();

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(catalog.metadata_calls(), calls_after_registration);
}

#[tokio::test]
async fn test_information_schema_keeps_last_good_snapshot_on_catalog_refresh_failure() {
    let healthy = Arc::new(MetadataListingCatalog::new());
    let unavailable = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .build();
    sql_context
        .register_catalog("healthy", healthy.clone())
        .await
        .unwrap();
    sql_context
        .register_catalog("unavailable", unavailable.clone())
        .await
        .unwrap();
    healthy.set_table_names(vec!["healthy_new"]);
    unavailable.fail_list_tables();

    let table_names = collect_string_column(
        &sql_context,
        "SELECT table_name FROM healthy.information_schema.tables \
         WHERE table_catalog = 'healthy' AND table_schema = 'default' \
         AND table_name = 'healthy_new'",
        "table_name",
    )
    .await;

    assert_eq!(table_names, vec!["healthy_new"]);
}

#[tokio::test]
async fn test_information_schema_backs_off_after_catalog_refresh_failure() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .build();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.fail_list_tables();

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();
    let calls_after_failure = catalog.metadata_calls();

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(catalog.metadata_calls(), calls_after_failure);
}

#[tokio::test]
async fn test_information_schema_times_out_pending_catalog_refresh() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .with_catalog_metadata_refresh_timeout(std::time::Duration::from_millis(20))
        .build();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.block_next_list_tables();

    tokio::time::timeout(
        std::time::Duration::from_millis(200),
        sql_context.sql("SELECT * FROM paimon.information_schema.tables"),
    )
    .await
    .expect("automatic catalog refresh must be time bounded")
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_information_schema_bounds_cross_catalog_refresh_concurrency() {
    let listing_concurrency = Arc::new(MetadataListingConcurrency::default());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .with_max_concurrent_catalog_metadata_refreshes(2)
        .build();
    for catalog_name in ["first", "second", "third", "fourth"] {
        sql_context
            .register_catalog(
                catalog_name,
                Arc::new(MetadataListingCatalog::with_listing_concurrency(
                    Arc::clone(&listing_concurrency),
                )),
            )
            .await
            .unwrap();
    }
    listing_concurrency.reset();

    sql_context
        .sql("SELECT * FROM first.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(listing_concurrency.maximum(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_information_schema_bounds_metadata_io_across_catalogs_and_databases() {
    let listing_concurrency = Arc::new(MetadataListingConcurrency::default());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .with_max_concurrent_catalog_metadata_refreshes(4)
        .with_max_concurrent_catalog_metadata_requests(3)
        .build();
    let mut catalogs = Vec::new();
    for catalog_name in ["first", "second", "third", "fourth"] {
        let catalog = Arc::new(MetadataListingCatalog::with_listing_concurrency(
            Arc::clone(&listing_concurrency),
        ));
        catalog.set_databases(vec!["db1", "db2", "db3", "db4"]);
        sql_context
            .register_catalog_with_default_db(catalog_name, catalog.clone(), None)
            .await
            .unwrap();
        catalogs.push((catalog_name, catalog));
    }
    for (catalog_name, catalog) in catalogs {
        for database in ["db1", "db2", "db3", "db4"] {
            let discovered = format!("{catalog_name}_{database}_new");
            catalog.set_table_names_for(database, vec![&discovered]);
        }
    }
    listing_concurrency.reset();

    sql_context
        .sql("SELECT * FROM first.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(listing_concurrency.maximum(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metadata_type_probes_use_global_bounded_parallelism() {
    let listing_concurrency = Arc::new(MetadataListingConcurrency::default());
    let catalog = Arc::new(MetadataListingCatalog::with_listing_concurrency(
        Arc::clone(&listing_concurrency),
    ));
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .with_max_concurrent_catalog_metadata_requests(3)
        .build();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.set_table_names(vec!["new1", "new2", "new3", "new4", "new5", "new6"]);
    listing_concurrency.reset();

    sql_context
        .sql("SELECT * FROM paimon.information_schema.tables")
        .await
        .unwrap();

    assert_eq!(listing_concurrency.maximum(), 3);
}

#[tokio::test]
async fn test_information_schema_does_not_hide_strict_object_refresh_failure() {
    let catalog = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .build();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    catalog.fail_list_tables();

    let error = sql_context
        .sql(
            "SELECT missing.* FROM paimon.information_schema.tables info \
             JOIN paimon.default.missing AS missing ON true",
        )
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("simulated metadata refresh failure"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_information_schema_refreshes_catalogs_concurrently() {
    let first = Arc::new(MetadataListingCatalog::new());
    let second = Arc::new(MetadataListingCatalog::new());
    let mut sql_context = SQLContext::builder()
        .with_catalog_metadata_refresh_ttl(std::time::Duration::ZERO)
        .build();
    sql_context
        .register_catalog("first", first.clone())
        .await
        .unwrap();
    sql_context
        .register_catalog("second", second.clone())
        .await
        .unwrap();
    first.block_next_list_tables();
    second.block_next_list_tables();
    let sql_context = Arc::new(sql_context);

    let query_context = Arc::clone(&sql_context);
    let query = tokio::spawn(async move {
        query_context
            .sql("SELECT * FROM first.information_schema.tables")
            .await
    });

    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        tokio::join!(
            first.blocked_list_tables_started.notified(),
            second.blocked_list_tables_started.notified()
        );
    })
    .await
    .expect("information schema catalog refreshes must start concurrently");
    first.release_blocked_list_tables.notify_one();
    second.release_blocked_list_tables.notify_one();
    query.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_select_branch_table_reads_branch_snapshot() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE TABLE paimon.default.branch_orders (id INT, name STRING)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.branch_orders VALUES (1, 'branch')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let identifier = Identifier::new("default", "branch_orders");
    let table = catalog.get_table(&identifier).await.unwrap();
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let tag_manager = TagManager::new(table.file_io().clone(), table.location().to_string());
    tag_manager.create("branch_base", &snapshot).await.unwrap();
    let branch_manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    branch_manager
        .create_branch_from_tag("b1", "branch_base")
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.default.branch_orders VALUES (2, 'main')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        collect_ids(&sql_context, "SELECT id FROM paimon.default.branch_orders").await,
        vec![1, 2]
    );
    assert_eq!(
        collect_ids(
            &sql_context,
            "SELECT id FROM paimon.default.branch_orders$branch_b1"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT snapshot_id FROM paimon.default.branch_orders$snapshots",
            "snapshot_id"
        )
        .await,
        vec![1, 2]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT snapshot_id FROM paimon.default.branch_orders$branch_b1$snapshots",
            "snapshot_id"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT record_count FROM paimon.default.branch_orders$files VERSION AS OF 'branch_base'",
            "record_count"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT record_count FROM paimon.default.branch_orders$branch_b1$files VERSION AS OF 'branch_base'",
            "record_count"
        )
        .await,
        vec![1]
    );
    assert!(!collect_string_column(
        &sql_context,
        "SELECT file_name FROM paimon.default.branch_orders$branch_b1$manifests",
        "file_name",
    )
    .await
    .is_empty());

    let branch_table = table.copy_with_branch("b1").await.unwrap();
    let write_builder = branch_table.new_write_builder();
    assert!(write_builder.new_write().is_err());
    assert!(write_builder.new_update(vec!["name".to_string()]).is_err());
    assert!(write_builder.new_delete().is_err());
    assert!(write_builder.try_new_commit().is_err());

    assert_sql_error_contains(
        &sql_context,
        "INSERT INTO paimon.default.branch_orders$branch_b1 VALUES (3, 'blocked')",
        "Writing to Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "INSERT INTO paimon.default.branch_orders$branch_main VALUES (3, 'blocked')",
        "Writing to Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "UPDATE paimon.default.branch_orders$branch_b1 SET name = 'blocked' WHERE id = 1",
        "UPDATE on Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "UPDATE paimon.default.branch_orders$branch_main SET name = 'blocked' WHERE id = 1",
        "UPDATE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "DELETE FROM paimon.default.branch_orders$branch_b1 WHERE id = 1",
        "DELETE on Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "DELETE FROM paimon.default.branch_orders$branch_main WHERE id = 1",
        "DELETE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "MERGE INTO paimon.default.branch_orders$branch_main AS target \
         USING (SELECT 1 AS id, 'blocked' AS name) AS source \
         ON target.id = source.id \
         WHEN MATCHED THEN UPDATE SET name = source.name",
        "MERGE INTO on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "TRUNCATE TABLE paimon.default.branch_orders$branch_main",
        "TRUNCATE TABLE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "ALTER TABLE paimon.default.branch_orders$branch_main ADD COLUMN blocked INT",
        "ALTER TABLE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "INSERT OVERWRITE paimon.default.branch_orders$branch_main \
         PARTITION (id = 1) SELECT 'blocked'",
        "INSERT OVERWRITE on Paimon branch 'main' is not supported",
    )
    .await;
}

/// A statistic the catalog never had reported to it has to read as NULL.
///
/// The alternative is what this used to do: declare the columns non-nullable and let
/// `Partition::UNKNOWN` through, so `$partitions` claimed the partition holds -1 rows and -1 files.
/// Reporting it as `0` instead would be worse still — that is a real measurement meaning empty.
#[tokio::test]
async fn test_partitions_system_table_shows_unreported_statistics_as_null() {
    let (_tmp, file_catalog) = create_test_env();
    let catalog = Arc::new(PartitionCatalog::new(file_catalog.clone()));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.default.unknown_stats_orders \
             (id INT, name STRING) PARTITIONED BY (id)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.unknown_stats_orders VALUES (1, 'a')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let sql = "SELECT record_count, file_size_in_bytes, file_count, total_buckets \
               FROM paimon.default.unknown_stats_orders$partitions";

    // Measured statistics still arrive as values, so a NULL below means unknown and not that the
    // column stopped being populated at all.
    let measured = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let measured = &measured[0];
    assert_eq!(measured.num_rows(), 1);
    for column in 0..4 {
        assert!(
            !measured.column(column).is_null(0),
            "column {column} should carry a measurement before the switch"
        );
    }

    catalog.set_unknown_statistics(true);

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    for column in 0..4 {
        assert!(
            batch.column(column).is_null(0),
            "column {column} was never measured and must read as NULL"
        );
    }

    // The partition itself is still registered; only its statistics are unknown.
    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.unknown_stats_orders$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
}

#[tokio::test]
async fn test_branch_partitions_system_table_reads_branch_snapshot() {
    let (_tmp, file_catalog) = create_test_env();
    let catalog = Arc::new(PartitionCatalog::new(file_catalog.clone()));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.default.branch_partition_orders \
             (id INT, name STRING) PARTITIONED BY (id)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.branch_partition_orders VALUES (1, 'branch')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let identifier = Identifier::new("default", "branch_partition_orders");
    let table = file_catalog.get_table(&identifier).await.unwrap();
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let tag_manager = TagManager::new(table.file_io().clone(), table.location().to_string());
    tag_manager
        .create("partition_branch_base", &snapshot)
        .await
        .unwrap();
    let branch_manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    branch_manager
        .create_branch_from_tag("b1", "partition_branch_base")
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.default.branch_partition_orders VALUES (2, 'main')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string(), "id=2".to_string()]
    );
    catalog.take_partition_identifiers();

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT created_by FROM paimon.default.branch_partition_orders$branch_b1$partitions",
            "created_by",
        )
        .await,
        vec!["catalog".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new(
            "default",
            "branch_partition_orders$branch_b1"
        )]
    );

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_main$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string(), "id=2".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new("default", "branch_partition_orders")]
    );

    catalog.set_fail_list_partitions(true);
    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_b1$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new(
            "default",
            "branch_partition_orders$branch_b1"
        )]
    );

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$partitions \
             VERSION AS OF 'partition_branch_base'",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_b1$partitions \
             VERSION AS OF 'partition_branch_base'",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert!(catalog.take_partition_identifiers().is_empty());
}

// ======================= DATABASE / SCHEMA =======================

#[tokio::test]
async fn test_database_statements() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context.sql("CREATE DATABASE analytics").await.unwrap();
    sql_context
        .sql("CREATE DATABASE IF NOT EXISTS analytics")
        .await
        .unwrap();

    let databases = collect_string_column(&sql_context, "SHOW DATABASES", "database_name").await;
    assert_eq!(databases, vec!["analytics", "default"]);

    sql_context
        .sql("CREATE TABLE analytics.events (id INT)")
        .await
        .unwrap();
    sql_context
        .sql("DROP DATABASE analytics CASCADE")
        .await
        .unwrap();
    sql_context
        .sql("DROP DATABASE IF EXISTS analytics")
        .await
        .unwrap();

    assert!(!catalog
        .list_databases()
        .await
        .unwrap()
        .contains(&"analytics".to_string()));
}

#[tokio::test]
async fn test_database_statements_reject_unsupported_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    assert_sql_error_contains(
        &sql_context,
        "SHOW DATABASES LIKE 'a%'",
        "SHOW DATABASES options are not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "CREATE DATABASE analytics LOCATION 'file:///tmp/analytics'",
        "CREATE DATABASE options are not supported",
    )
    .await;

    catalog
        .create_database("keep_me", false, Default::default())
        .await
        .unwrap();
    assert_sql_error_contains(
        &sql_context,
        "DROP DATABASE keep_me PURGE",
        "DROP DATABASE options are not supported",
    )
    .await;

    assert!(catalog
        .list_databases()
        .await
        .unwrap()
        .contains(&"keep_me".to_string()));
}

#[tokio::test]
async fn test_use_catalog_qualified_database() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("cat1", catalog1.clone())
        .await
        .unwrap();
    sql_context
        .register_catalog("cat2", catalog2.clone())
        .await
        .unwrap();

    sql_context
        .sql("CREATE DATABASE cat2.analytics")
        .await
        .unwrap();
    sql_context.sql("USE cat2.analytics").await.unwrap();
    sql_context
        .sql("CREATE TABLE events (id INT)")
        .await
        .unwrap();

    assert!(catalog1.list_tables("analytics").await.is_err());
    assert_eq!(
        catalog2.list_tables("analytics").await.unwrap(),
        vec!["events"]
    );
    assert_sql_error_contains(
        &sql_context,
        "USE missing_database",
        "Database missing_database does not exist",
    )
    .await;
}

#[tokio::test]
async fn test_create_schema() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    let databases = catalog.list_databases().await.unwrap();
    assert!(
        databases.contains(&"test_db".to_string()),
        "Database test_db should exist after CREATE SCHEMA"
    );
}

#[tokio::test]
async fn test_drop_schema() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("drop_me", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql("DROP SCHEMA paimon.drop_me CASCADE")
        .await
        .expect("DROP SCHEMA should succeed");

    let databases = catalog.list_databases().await.unwrap();
    assert!(
        !databases.contains(&"drop_me".to_string()),
        "Database drop_me should not exist after DROP SCHEMA"
    );
}

#[tokio::test]
async fn test_schema_names_via_catalog_provider() {
    let (_tmp, catalog) = create_test_env();
    catalog
        .create_database("db_a", false, Default::default())
        .await
        .unwrap();
    catalog
        .create_database("db_b", false, Default::default())
        .await
        .unwrap();

    let provider = PaimonCatalogProvider::try_new(
        None,
        catalog.clone(),
        Default::default(),
        Default::default(),
        None,
    )
    .await
    .unwrap();

    let names = provider.schema_names();
    assert!(names.contains(&"db_a".to_string()));
    assert!(names.contains(&"db_b".to_string()));
}

// ======================= CREATE TABLE =======================

#[tokio::test]
async fn test_create_table() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.users (
                id INT NOT NULL,
                name STRING,
                age INT,
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        tables.contains(&"users".to_string()),
        "Table users should exist after CREATE TABLE"
    );

    // Verify schema
    let table = catalog
        .get_table(&Identifier::new("mydb", "users"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.primary_keys(), &["id"]);
}

#[tokio::test]
async fn test_create_table_with_blob_type() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.assets (
                id INT NOT NULL,
                payload BLOB
            ) WITH (
                'data-evolution.enabled' = 'true',
                'row-tracking.enabled' = 'true'
            )",
        )
        .await
        .expect("CREATE TABLE with BLOB should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "assets"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 2);
    assert!(schema.primary_keys().is_empty());
    assert_eq!(
        *schema.fields()[1].data_type(),
        DataType::Blob(BlobType::new())
    );
}

#[tokio::test]
async fn test_create_table_with_partition() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.events (
                id INT NOT NULL,
                name STRING,
                dt STRING,
                PRIMARY KEY (id, dt)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '2')",
        )
        .await
        .expect("CREATE TABLE with partition should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "events"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.partition_keys(), &["dt"]);
    assert_eq!(schema.primary_keys(), &["id", "dt"]);
    assert_eq!(
        schema.options().get("bucket"),
        Some(&"2".to_string()),
        "Table option 'bucket' should be preserved"
    );
}

#[tokio::test]
async fn test_create_table_partitioned_by_rejects_typed_columns() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.mydb.events (
                id INT NOT NULL,
                dt STRING
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .expect_err("PARTITIONED BY with typed columns should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("should not specify a type"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let sql = "CREATE TABLE IF NOT EXISTS paimon.mydb.t1 (
        id INT NOT NULL
    )";

    // First create should succeed
    sql_context
        .sql(sql)
        .await
        .expect("First CREATE should succeed");

    // Second create with IF NOT EXISTS should also succeed
    sql_context
        .sql(sql)
        .await
        .expect("Second CREATE with IF NOT EXISTS should succeed");
}

#[tokio::test]
async fn test_create_object_table_delta_does_not_claim_system_table_support() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.objects (id BIGINT) \
             WITH ('type' = 'object-table')",
        )
        .await
        .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("mydb").unwrap();
    assert!(schema.table_exist("objects"));
    assert!(!schema.table_exist("objects$snapshots"));
}

#[tokio::test]
async fn test_create_table_if_not_exists_noop_does_not_overwrite_object_capability() {
    let (_tmp, catalog) = create_test_env();
    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();
    let object_schema = paimon::spec::Schema::builder()
        .column("id", paimon::spec::DataType::BigInt(Default::default()))
        .option("type", "object-table")
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "objects"), object_schema, false)
        .await
        .unwrap();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE TABLE IF NOT EXISTS paimon.mydb.objects (id BIGINT)")
        .await
        .unwrap();

    let provider = sql_context.ctx().catalog("paimon").unwrap();
    let schema = provider.schema("mydb").unwrap();
    assert!(schema.table_exist("objects"));
    assert!(!schema.table_exist("objects$snapshots"));
}

#[tokio::test]
async fn test_create_external_table_rejected() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let result = sql_context
        .sql(
            "CREATE EXTERNAL TABLE paimon.mydb.bad (
                id INT NOT NULL
            ) STORED AS PARQUET
            LOCATION '/some/path'",
        )
        .await;

    assert!(result.is_err(), "CREATE EXTERNAL TABLE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("CREATE EXTERNAL TABLE is not supported"),
        "Error should mention CREATE EXTERNAL TABLE is not supported, got: {err_msg}"
    );
}

// ======================= CREATE TABLE with complex types =======================

#[tokio::test]
async fn test_create_table_with_array_and_map() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.complex_types (
                id INT NOT NULL,
                tags ARRAY<STRING>,
                props MAP(STRING, INT),
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE with ARRAY and MAP should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "complex_types"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.primary_keys(), &["id"]);

    // Verify ARRAY<STRING> column
    let tags_field = &schema.fields()[1];
    assert_eq!(tags_field.name(), "tags");
    assert_eq!(
        *tags_field.data_type(),
        DataType::Array(ArrayType::new(
            DataType::VarChar(VarCharType::string_type())
        ))
    );

    // Verify MAP(STRING, INT) column
    let props_field = &schema.fields()[2];
    assert_eq!(props_field.name(), "props");
    assert_eq!(
        *props_field.data_type(),
        DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type())
                .copy_with_nullable(false)
                .unwrap(),
            DataType::Int(IntType::new()),
        ))
    );
}

#[tokio::test]
async fn test_create_table_with_row_type() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.row_table (
                id INT NOT NULL,
                address STRUCT<city STRING, zip INT>,
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE with STRUCT should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "row_table"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 2);

    // Verify STRUCT<city STRING, zip INT> column
    let address_field = &schema.fields()[1];
    assert_eq!(address_field.name(), "address");
    if let DataType::Row(row) = address_field.data_type() {
        assert_eq!(row.fields().len(), 2);
        assert_eq!(row.fields()[0].name(), "city");
        assert!(matches!(row.fields()[0].data_type(), DataType::VarChar(_)));
        assert_eq!(row.fields()[1].name(), "zip");
        assert!(matches!(row.fields()[1].data_type(), DataType::Int(_)));
    } else {
        panic!("expected Row type for address column");
    }
}

// ======================= DROP TABLE =======================

#[tokio::test]
async fn test_drop_table() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    // Create a table first
    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "to_drop"), schema, false)
        .await
        .unwrap();

    assert!(catalog
        .list_tables("mydb")
        .await
        .unwrap()
        .contains(&"to_drop".to_string()));

    sql_context
        .sql("DROP TABLE paimon.mydb.to_drop")
        .await
        .expect("DROP TABLE should succeed");

    assert!(
        !catalog
            .list_tables("mydb")
            .await
            .unwrap()
            .contains(&"to_drop".to_string()),
        "Table should not exist after DROP TABLE"
    );
}

// ======================= ALTER TABLE =======================

#[tokio::test]
async fn test_alter_table_add_column() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .column(
            "name",
            paimon::spec::DataType::VarChar(paimon::spec::VarCharType::string_type()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "alter_test"), schema, false)
        .await
        .unwrap();

    sql_context
        .sql("ALTER TABLE paimon.mydb.alter_test ADD COLUMN age INT")
        .await
        .expect("ALTER TABLE ADD COLUMN should succeed");

    // The new column is appended to the table schema.
    let table = catalog
        .get_table(&Identifier::new("mydb", "alter_test"))
        .await
        .unwrap();
    let names: Vec<&str> = table.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "name", "age"]);
}

#[tokio::test]
async fn test_alter_table_update_column_type_and_nullability() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    let identifier = Identifier::new("mydb", "alter_column_test");

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();
    let schema = paimon::spec::Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("alter-column-null-to-not-null.disabled", "false")
        .build()
        .unwrap();
    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();

    sql_context
        .sql("ALTER TABLE mydb.alter_column_test ALTER COLUMN value SET NOT NULL")
        .await
        .expect("ALTER COLUMN SET NOT NULL should succeed");
    let table = catalog.get_table(&identifier).await.unwrap();
    assert!(!table.schema().fields()[1].data_type().is_nullable());

    sql_context
        .sql("ALTER TABLE mydb.alter_column_test ALTER COLUMN value TYPE BIGINT")
        .await
        .expect("ALTER COLUMN TYPE should succeed");
    let table = catalog.get_table(&identifier).await.unwrap();
    assert!(matches!(
        table.schema().fields()[1].data_type(),
        DataType::BigInt(_)
    ));
    assert!(!table.schema().fields()[1].data_type().is_nullable());

    sql_context
        .sql("ALTER TABLE mydb.alter_column_test ALTER COLUMN value DROP NOT NULL")
        .await
        .expect("ALTER COLUMN DROP NOT NULL should succeed");
    let table = catalog.get_table(&identifier).await.unwrap();
    assert!(table.schema().fields()[1].data_type().is_nullable());
}

#[tokio::test]
async fn test_alter_column_preserves_quoted_identifier() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.mydb")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.quoted_column (
                id INT,
                \"MixedCase\" INT
            )",
        )
        .await
        .expect("CREATE TABLE should preserve the quoted column name");

    sql_context
        .sql(
            "ALTER TABLE paimon.mydb.quoted_column
             ALTER COLUMN \"MixedCase\" TYPE BIGINT",
        )
        .await
        .expect("ALTER COLUMN should resolve the quoted column name");

    let table = catalog
        .get_table(&Identifier::new("mydb", "quoted_column"))
        .await
        .unwrap();
    let field = table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "MixedCase")
        .expect("quoted column should retain its exact name");
    assert!(matches!(field.data_type(), DataType::BigInt(_)));
}

#[tokio::test]
async fn test_alter_table_rename() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "old_name"), schema, false)
        .await
        .unwrap();

    sql_context
        .sql("ALTER TABLE mydb.old_name RENAME TO new_name")
        .await
        .expect("ALTER TABLE RENAME should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        !tables.contains(&"old_name".to_string()),
        "old_name should not exist after rename"
    );
    assert!(
        tables.contains(&"new_name".to_string()),
        "new_name should exist after rename"
    );
}

#[tokio::test]
async fn test_ddl_context_delegates_select() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "t1"), schema, false)
        .await
        .unwrap();

    // SELECT should be delegated to DataFusion
    let df = sql_context
        .sql("SELECT * FROM paimon.mydb.t1")
        .await
        .expect("SELECT should be delegated to DataFusion");

    let batches = df.collect().await.expect("SELECT should execute");
    // Empty table, but should succeed
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table should return 0 rows");
}

// ======================= MULTI-CATALOG =======================

#[tokio::test]
async fn test_multi_catalog_register_and_query() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    ctx.sql("CREATE SCHEMA cat1.db1").await.unwrap();
    ctx.sql("CREATE SCHEMA cat2.db2").await.unwrap();

    ctx.sql("CREATE TABLE cat1.db1.t (id INT NOT NULL, name STRING, PRIMARY KEY (id))")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE cat2.db2.t (id INT NOT NULL, value STRING, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.sql("INSERT INTO cat1.db1.t VALUES (1, 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO cat2.db2.t VALUES (2, 'hello')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM cat1.db1.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let batches = ctx
        .sql("SELECT id, value FROM cat2.db2.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_set_current_catalog() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    ctx.sql("CREATE SCHEMA cat1.mydb").await.unwrap();
    ctx.sql("CREATE TABLE cat1.mydb.t (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.set_current_catalog("cat1").await.unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // Unqualified query should resolve against cat1.mydb
    let df = ctx.sql("SELECT * FROM t").await;
    assert!(
        df.is_ok(),
        "Unqualified table should resolve via current catalog/database"
    );

    // Switching to unknown catalog should fail
    let err = ctx.set_current_catalog("nonexistent").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_set_default_catalog_via_datafusion_config() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    // Create a table in cat2
    ctx.sql("CREATE SCHEMA cat2.mydb").await.unwrap();
    ctx.sql("CREATE TABLE cat2.mydb.t (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))")
        .await
        .unwrap();
    ctx.sql("INSERT INTO cat2.mydb.t VALUES (1, 'hello')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Switch default catalog via raw DataFusion SET instead of set_current_catalog()
    ctx.sql("SET datafusion.catalog.default_catalog = 'cat2'")
        .await
        .unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // Unqualified query should now resolve against cat2.mydb
    let batches = ctx
        .sql("SELECT id, name FROM t")
        .await
        .expect("Unqualified table should resolve via DataFusion default_catalog config")
        .collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // DDL on unqualified name should also go to cat2.
    // CREATE TABLE in mydb should succeed because cat2.mydb exists.
    ctx.sql("CREATE TABLE mydb.t2 (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE should resolve against cat2 after SET default_catalog");

    // Verify the table was created in cat2 by querying with fully qualified name
    let df = ctx.sql("SELECT * FROM cat2.mydb.t2").await;
    assert!(
        df.is_ok(),
        "Table t2 should exist in cat2.mydb after unqualified CREATE TABLE"
    );
}

#[tokio::test]
async fn test_first_registered_catalog_is_default() {
    let (_tmp, catalog) = create_test_env();
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();

    ctx.sql("CREATE SCHEMA paimon.mydb").await.unwrap();
    ctx.sql("CREATE TABLE paimon.mydb.t (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.set_current_database("mydb").await.unwrap();

    // Should resolve to paimon.mydb.t without calling set_current_catalog
    let df = ctx.sql("SELECT * FROM t").await;
    assert!(
        df.is_ok(),
        "First registered catalog should be the default for unqualified queries"
    );
}

#[tokio::test]
async fn test_one_part_table_name_uses_current_database() {
    let (_tmp, catalog) = create_test_env();
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // 1-part name: "users" should resolve to paimon.mydb.users
    ctx.sql(
        "CREATE TABLE users (
            id INT NOT NULL,
            name STRING,
            PRIMARY KEY (id)
        )",
    )
    .await
    .expect("CREATE TABLE with 1-part name should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        tables.contains(&"users".to_string()),
        "Table should be created in the current database"
    );

    // SELECT with 1-part name should also work
    let df = ctx.sql("SELECT * FROM users").await;
    assert!(
        df.is_ok(),
        "SELECT with 1-part name should resolve correctly"
    );
}

// ======================= TEMP TABLE =======================

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

#[tokio::test]
async fn test_register_temp_table_fully_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Fully qualified: catalog.database.table
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.my_temp", Arc::new(mem_table))
        .unwrap();

    // Query the temp table via SQL
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_temp")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_register_temp_table_database_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
                Some("charlie"),
                Some("dave"),
            ])),
        ],
    )
    .unwrap();

    // Database-qualified: database.table (uses current catalog)
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_db.users", Arc::new(mem_table))
        .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM paimon.my_db.users WHERE id > 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_not_filter_pushdown_keeps_sql_null_semantics() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("my_db", false, Default::default())
        .await
        .unwrap();
    ctx.sql("CREATE TABLE paimon.my_db.t (id INT, name STRING)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO paimon.my_db.t VALUES (1, 'one'), (2, 'two'), (NULL, 'nil')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT id FROM paimon.my_db.t WHERE NOT (id = 1) ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let values = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .and_then(|column| {
                    column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int32Array>()
                })
                .expect("id column");
            (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![2]);
}

#[tokio::test]
async fn test_register_temp_table_bare() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a database and set it as current database
    ctx.sql("CREATE DATABASE paimon.my_db").await.unwrap();
    ctx.set_current_database("my_db").await.unwrap();

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Bare: just table name (uses current catalog + current database)
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_temp", Arc::new(mem_table))
        .unwrap();

    // Query via paimon.my_db.my_temp
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_temp")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_register_temp_table_unknown_catalog() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));

    let mem_table = MemTable::try_new(schema, vec![vec![]]).unwrap();
    let result = ctx.register_temp_table("nonexistent.my_db.t", Arc::new(mem_table));
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unknown catalog"));
}

#[tokio::test]
async fn test_deregister_temp_table() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();

    let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.my_temp", Arc::new(mem_table))
        .unwrap();

    // Deregister with flexible name
    ctx.deregister_temp_table("paimon.my_db.my_temp").unwrap();

    // Query should fail
    let result = ctx.sql("SELECT * FROM paimon.my_db.my_temp").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_temp_tables_in_same_database() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema1 = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();

    let schema2 = Arc::new(Schema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Int32,
        false,
    )]));
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();

    let mem_table = MemTable::try_new(schema1, vec![vec![batch1]]).unwrap();
    ctx.register_temp_table("my_db.t1", Arc::new(mem_table))
        .unwrap();
    let mem_table = MemTable::try_new(schema2, vec![vec![batch2]]).unwrap();
    ctx.register_temp_table("my_db.t2", Arc::new(mem_table))
        .unwrap();

    // Both should be queryable
    let rows1 = ctx
        .sql("SELECT * FROM paimon.my_db.t1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows1, 2);

    let rows2 = ctx
        .sql("SELECT * FROM paimon.my_db.t2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows2, 3);
}

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::test]
async fn test_create_temporary_table_as_select() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a temporary table via SQL
    ctx.sql("CREATE TEMPORARY TABLE paimon.my_db.source AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")
        .await
        .unwrap();

    // Query the temporary table
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.source ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_drop_temporary_table() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a temporary table
    ctx.sql("CREATE TEMPORARY TABLE paimon.my_db.source AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")
        .await
        .unwrap();

    // Verify it exists
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.source ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // Drop it
    ctx.sql("DROP TEMPORARY TABLE paimon.my_db.source")
        .await
        .unwrap();

    // Verify it no longer exists
    let result = ctx.sql("SELECT * FROM paimon.my_db.source").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_temporary_table_if_exists() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // DROP TEMPORARY TABLE on non-existent table with IF EXISTS should succeed
    ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.my_db.nonexistent")
        .await
        .unwrap();

    // Without IF EXISTS, it should fail
    let result = ctx
        .sql("DROP TEMPORARY TABLE paimon.my_db.nonexistent")
        .await;
    assert!(result.is_err());
}

// ======================= TEMP VIEW =======================

#[tokio::test]
async fn test_create_temporary_view_fully_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.users", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW paimon.my_db.my_view AS SELECT * FROM paimon.my_db.users WHERE id > 0")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_create_temporary_view_database_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.data", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_db.summary AS SELECT value FROM paimon.my_db.data WHERE value > 5")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT value FROM paimon.my_db.summary WHERE value > 15")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_create_temporary_view_bare() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    ctx.sql("CREATE DATABASE paimon.my_db").await.unwrap();
    ctx.set_current_database("my_db").await.unwrap();

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![100, 200]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_db.source", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_view AS SELECT id FROM paimon.my_db.source")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_drop_temporary_view() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.source", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW paimon.my_db.my_view AS SELECT * FROM paimon.my_db.source")
        .await
        .unwrap();

    // Drop via SQL
    ctx.sql("DROP TEMPORARY VIEW paimon.my_db.my_view")
        .await
        .unwrap();

    let result = ctx.sql("SELECT * FROM paimon.my_db.my_view").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_temporary_views_in_same_database() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema1 = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema1, vec![vec![batch1]]).unwrap();
    ctx.register_temp_table("paimon.my_db.t1", Arc::new(mem_table))
        .unwrap();

    let schema2 = Arc::new(Schema::new(vec![ArrowField::new(
        "name",
        ArrowDataType::Utf8,
        true,
    )]));
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("x"),
            Some("y"),
            Some("z"),
        ]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema2, vec![vec![batch2]]).unwrap();
    ctx.register_temp_table("paimon.my_db.t2", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_db.v1 AS SELECT id FROM paimon.my_db.t1")
        .await
        .unwrap();
    ctx.sql("CREATE TEMPORARY VIEW my_db.v2 AS SELECT name FROM paimon.my_db.t2")
        .await
        .unwrap();

    let rows1 = ctx
        .sql("SELECT * FROM paimon.my_db.v1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows1, 2);

    let rows2 = ctx
        .sql("SELECT * FROM paimon.my_db.v2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows2, 3);
}

// ======================= SHOW CREATE TABLE =======================

/// Collect the `definition` column from `SHOW CREATE TABLE` output as a String.
async fn collect_definition(sql_context: &SQLContext, table_ref: &str) -> String {
    let rows = sql_context
        .sql(&format!("SHOW CREATE TABLE {}", table_ref))
        .await
        .expect("SHOW CREATE TABLE should plan")
        .collect()
        .await
        .expect("SHOW CREATE TABLE should execute");
    assert_eq!(
        rows.len(),
        1,
        "SHOW CREATE TABLE should return exactly one row"
    );
    let row = &rows[0];
    assert_eq!(
        row.num_rows(),
        1,
        "SHOW CREATE TABLE should return exactly one row"
    );
    let val = row.column(3); // definition is the 4th column
    let def = val
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("definition column should be a StringArray")
        .value(0);
    def.to_string()
}

#[tokio::test]
async fn test_show_create_table_simple() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR(100))")
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("CREATE TABLE \"test_db\".\"t\""),
        "definition should start with CREATE TABLE \"test_db\".\"t\", got: {definition}"
    );
    assert!(
        definition.contains("\"id\" INT"),
        "definition should contain `\"id\" INT`, got: {definition}"
    );
    assert!(
        definition.contains("\"name\" VARCHAR("),
        "definition should contain `\"name\" VARCHAR(...)`, got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_with_primary_key() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("PRIMARY KEY (\"id\")"),
        "definition should contain PRIMARY KEY (\"id\"), got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_with_partition_and_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) \
             PARTITIONED BY (pt) WITH ('bucket' = '4', 'file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("PARTITIONED BY (\"pt\")"),
        "definition should contain PARTITIONED BY (\"pt\"), got: {definition}"
    );
    assert!(
        definition.contains("'bucket' = '4'"),
        "definition should contain bucket option, got: {definition}"
    );
    assert!(
        definition.contains("'file.format' = 'parquet'"),
        "definition should contain file.format option, got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_excludes_session_dynamic_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) \
             PARTITIONED BY (pt) WITH ('file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");
    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 'one', 1)")
        .await
        .expect("INSERT should plan")
        .collect()
        .await
        .expect("INSERT should execute");
    sql_context
        .sql("CALL sys.create_tag(table => 'test_db.t', tag => 'before_age')")
        .await
        .expect("CREATE TAG should succeed");
    sql_context
        .sql("ALTER TABLE paimon.test_db.t ADD COLUMN age INT")
        .await
        .expect("ALTER TABLE should succeed");
    sql_context
        .sql("SET 'paimon.scan.version' = 'before_age'")
        .await
        .expect("SET scan.version should succeed");
    sql_context
        .sql("SET 'paimon.blob-as-descriptor' = 'true'")
        .await
        .expect("SET blob-as-descriptor should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("'file.format' = 'parquet'"),
        "definition should keep persisted table options, got: {definition}"
    );
    assert!(
        definition.contains("\"age\" INT"),
        "definition should use current persisted schema, got: {definition}"
    );
    for dynamic_option in ["scan.version", "blob-as-descriptor"] {
        assert!(
            !definition.contains(dynamic_option),
            "definition should not contain session dynamic option {dynamic_option}, got: {definition}"
        );
    }
}

#[tokio::test]
async fn test_dynamic_scan_ignores_current_show_create_unsupported_type() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT)")
        .await
        .expect("CREATE TABLE should succeed");
    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1)")
        .await
        .expect("INSERT should plan")
        .collect()
        .await
        .expect("INSERT should execute");
    sql_context
        .sql("CALL sys.create_tag(table => 'test_db.t', tag => 'before_time')")
        .await
        .expect("CREATE TAG should succeed");

    let identifier = Identifier::new("test_db", "t");
    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::add_column(
                "unsupported_col".to_string(),
                DataType::Time(TimeType::new(3).unwrap()),
            )],
            false,
        )
        .await
        .expect("ALTER TABLE should add unsupported SHOW CREATE type");

    sql_context
        .sql("SET 'paimon.scan.version' = 'before_time'")
        .await
        .expect("SET scan.version should succeed");

    let rows = sql_context
        .sql("SELECT * FROM paimon.test_db.t")
        .await
        .expect("dynamic scan should plan with historical schema")
        .collect()
        .await
        .expect("dynamic scan should execute");
    assert_eq!(rows[0].schema().fields().len(), 1);
    assert_eq!(rows[0].schema().field(0).name(), "id");
    let row_count: usize = rows.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(row_count, 1);
}

#[tokio::test]
async fn test_show_create_table_various_types() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (\
             a BOOLEAN, \
             b TINYINT, \
             c SMALLINT, \
             d BIGINT, \
             e DECIMAL(10, 2), \
             f DOUBLE, \
             g FLOAT, \
             h DATE, \
             i TIMESTAMP(3), \
             j BLOB) \
             WITH (\
                 'data-evolution.enabled' = 'true', \
                 'row-tracking.enabled' = 'true'\
             )",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    for needle in [
        "\"a\" BOOLEAN",
        "\"b\" TINYINT",
        "\"c\" SMALLINT",
        "\"d\" BIGINT",
        "\"e\" DECIMAL(10, 2)",
        "\"f\" DOUBLE",
        "\"g\" FLOAT",
        "\"h\" DATE",
        "\"i\" TIMESTAMP(3)",
        "\"j\" BLOB",
    ] {
        assert!(
            definition.contains(needle),
            "definition should contain `{needle}`, got: {definition}"
        );
    }
}

/// Assert that two `TableSchema`s are equivalent for round-trip purposes:
/// same fields (id, name, type), same primary keys, same partition keys.
///
/// We do not compare `options` because the CREATE TABLE path may inject
/// catalog defaults (e.g. `bucket`) that the user did not specify; the
/// schema fields and key columns are what the DDL must preserve.
fn assert_schema_equivalent(left: &paimon::spec::TableSchema, right: &paimon::spec::TableSchema) {
    assert_eq!(
        left.fields().len(),
        right.fields().len(),
        "field count mismatch\nleft  (original): {:?}\nright (recreated): {:?}",
        left.fields(),
        right.fields()
    );
    for (lf, rf) in left.fields().iter().zip(right.fields().iter()) {
        assert_eq!(
            lf.id(),
            rf.id(),
            "field id mismatch for `{}`: {} vs {}",
            lf.name(),
            lf.id(),
            rf.id()
        );
        assert_eq!(
            lf.name(),
            rf.name(),
            "field name mismatch: `{}` vs `{}`",
            lf.name(),
            rf.name()
        );
        assert_eq!(
            lf.data_type(),
            rf.data_type(),
            "field type mismatch for `{}`: {:?} vs {:?}",
            lf.name(),
            lf.data_type(),
            rf.data_type()
        );
    }
    assert_eq!(
        left.primary_keys(),
        right.primary_keys(),
        "primary keys mismatch: {:?} vs {:?}",
        left.primary_keys(),
        right.primary_keys()
    );
    assert_eq!(
        left.partition_keys(),
        right.partition_keys(),
        "partition keys mismatch: {:?} vs {:?}",
        left.partition_keys(),
        right.partition_keys()
    );
}

/// Round-trip test: the DDL returned by `SHOW CREATE TABLE` must be executable
/// by paimon-rust's own `CREATE TABLE` parser and reproduce an equivalent
/// schema (fields, primary keys, partition keys).
///
/// This guards against regressions where the rendered DDL drifts away from
/// what the parser accepts (e.g. `ROW<name: type>` vs `STRUCT<name type>`,
/// `MAP<k: v>` vs `MAP(k, v)`, or dropped `NOT NULL`).
#[tokio::test]
async fn test_show_create_table_round_trip() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t1 (\
             id INT NOT NULL, \
             name VARCHAR NOT NULL, \
             tags ARRAY<STRING>, \
             props MAP(INT, VARCHAR), \
             addr STRUCT<city VARCHAR, zip VARCHAR>, \
             meta STRUCT<kv MAP(STRING, STRING), tags ARRAY<INT>>, \
             PRIMARY KEY (id)) \
             PARTITIONED BY (name) \
             WITH ('bucket' = '2', 'file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let identifier = Identifier::new("test_db", "t1");
    let original = catalog.get_table(&identifier).await.expect("table exists");
    let original_schema = original.schema().clone();

    let definition = collect_definition(&sql_context, "paimon.test_db.t1").await;
    // The DDL is rendered as `CREATE TABLE test_db.t1 (...)` without the
    // catalog prefix; paimon is the default catalog so this resolves back
    // to the same catalog/database.
    assert!(
        definition.starts_with("CREATE TABLE \"test_db\".\"t1\""),
        "definition should start with `CREATE TABLE \"test_db\".\"t1\"`, got: {definition}"
    );

    catalog
        .drop_table(&identifier, false)
        .await
        .expect("drop should succeed");

    sql_context
        .sql(&definition)
        .await
        .expect("DDL should re-execute")
        .collect()
        .await
        .expect("DDL should execute");

    let recreated = catalog
        .get_table(&identifier)
        .await
        .expect("recreated table exists");
    assert_schema_equivalent(&original_schema, recreated.schema());
}

#[tokio::test]
async fn test_show_create_table_round_trip_with_quoted_identifiers_and_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    let identifier = Identifier::new("test_db", "select");
    let schema = paimon::spec::Schema::builder()
        .column("group", DataType::Int(IntType::with_nullable(false)))
        .column("order", DataType::Int(IntType::with_nullable(false)))
        .column("a\"b,c", DataType::Int(IntType::new()))
        .column(
            "nested",
            DataType::Row(paimon::spec::RowType::new(vec![
                paimon::spec::DataField::new(
                    0,
                    "from".to_string(),
                    DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                ),
            ])),
        )
        .column(
            "ts_ltz",
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
        )
        .column("fixed_char", DataType::Char(CharType::new(7).unwrap()))
        .column(
            "bounded_varchar",
            DataType::VarChar(VarCharType::new(42).unwrap()),
        )
        .column(
            "fixed_binary",
            DataType::Binary(BinaryType::new(8).unwrap()),
        )
        .column(
            "bounded_varbinary",
            DataType::VarBinary(VarBinaryType::try_new(true, 32).unwrap()),
        )
        .primary_key(vec!["group", "order"])
        .partition_keys(vec!["a\"b,c"])
        .option("comment", "Bob's table")
        .build()
        .expect("schema should build");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("table should be created");
    let original = catalog.get_table(&identifier).await.expect("table exists");
    let original_schema = original.schema().clone();

    let definition = collect_definition(&sql_context, "paimon.test_db.\"select\"").await;
    assert!(
        definition.starts_with("CREATE TABLE \"test_db\".\"select\""),
        "definition should quote table identifiers, got: {definition}"
    );
    assert!(
        definition.contains("\"order\" INT NOT NULL"),
        "definition should quote column identifiers, got: {definition}"
    );
    assert!(
        definition.contains("PRIMARY KEY (\"group\", \"order\")"),
        "definition should quote primary key identifiers, got: {definition}"
    );
    assert!(
        definition.contains("\"a\"\"b,c\" INT"),
        "definition should escape quoted column identifiers, got: {definition}"
    );
    assert!(
        definition.contains("PARTITIONED BY (\"a\"\"b,c\")"),
        "definition should escape quoted partition identifiers, got: {definition}"
    );
    assert!(
        definition.contains("STRUCT<\"from\" VARCHAR"),
        "definition should quote nested struct field identifiers, got: {definition}"
    );
    assert!(
        definition.contains("'comment' = 'Bob''s table'"),
        "definition should escape string literals, got: {definition}"
    );
    assert!(
        definition.contains("\"ts_ltz\" TIMESTAMP(3) WITH TIME ZONE"),
        "definition should render TIMESTAMP WITH TIME ZONE for LTZ, got: {definition}"
    );
    assert!(
        definition.contains("\"fixed_char\" CHAR(7)"),
        "definition should preserve CHAR length, got: {definition}"
    );
    assert!(
        definition.contains("\"bounded_varchar\" VARCHAR(42)"),
        "definition should preserve VARCHAR length, got: {definition}"
    );
    assert!(
        definition.contains("\"fixed_binary\" BINARY(8)"),
        "definition should preserve BINARY length, got: {definition}"
    );
    assert!(
        definition.contains("\"bounded_varbinary\" VARBINARY(32)"),
        "definition should preserve VARBINARY length, got: {definition}"
    );

    catalog
        .drop_table(&identifier, false)
        .await
        .expect("drop should succeed");

    sql_context
        .sql(&definition)
        .await
        .expect("DDL should re-execute")
        .collect()
        .await
        .expect("DDL should execute");

    let recreated = catalog
        .get_table(&identifier)
        .await
        .expect("recreated table exists");
    assert_schema_equivalent(&original_schema, recreated.schema());
}

#[tokio::test]
async fn test_show_create_table_rejects_non_round_trippable_types() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    for (table_name, data_type, type_name) in [
        ("time_t", DataType::Time(TimeType::new(3).unwrap()), "TIME"),
        (
            "multiset_t",
            DataType::Multiset(MultisetType::new(DataType::Int(IntType::new()))),
            "MULTISET",
        ),
        (
            "vector_t",
            DataType::Vector(VectorType::new(4, DataType::Float(FloatType::new())).unwrap()),
            "VECTOR",
        ),
    ] {
        let identifier = Identifier::new("test_db", table_name);
        let schema = paimon::spec::Schema::builder()
            .column("unsupported_col", data_type)
            .build()
            .expect("schema should build");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .expect("table should be created");

        sql_context
            .sql("SET 'paimon.blob-as-descriptor' = 'true'")
            .await
            .expect("SET blob-as-descriptor should succeed");

        let err = sql_context
            .sql(&format!("SHOW CREATE TABLE paimon.test_db.{table_name}"))
            .await
            .expect_err("SHOW CREATE TABLE should reject unsupported type");
        assert!(
            err.to_string().contains(type_name),
            "error should mention {type_name}, got: {err}"
        );
    }
}

#[tokio::test]
async fn test_vector_search_plans_vector_column_without_show_create_support() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    let identifier = Identifier::new("default", "vector_t");
    let schema = paimon::spec::Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Vector(VectorType::new(2, DataType::Float(FloatType::new())).unwrap()),
        )
        .build()
        .unwrap();

    catalog
        .create_table(&identifier, schema, false)
        .await
        .unwrap();

    sql_context
        .sql(
            "SELECT * FROM vector_search(\
             'paimon.default.vector_t', 'embedding', '[1.0, 2.0]', 1)",
        )
        .await
        .expect("vector_search should plan without requiring SHOW CREATE TABLE support");
}
