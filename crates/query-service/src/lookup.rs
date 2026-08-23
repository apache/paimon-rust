// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use arrow_array::{Array, BinaryArray, RecordBatch};
use base64::Engine;
use futures::lock::Mutex as AsyncMutex;
use futures::{StreamExt, TryStreamExt};
use moka::future::Cache;
use paimon::spec::BlobDescriptor;
use paimon::{Catalog, Table};
use tokio::sync::Notify;

use crate::error::{LookupError, Result};
use crate::key::{
    build_batch_predicate, normalized_key_from_batch, prepare_keys, NormalizedKey, PreparedKey,
};
use crate::model::{
    BatchGetRequest, BatchGetResponse, BlobDescriptorDto, DescriptorFormat, LookupResult,
    LookupScanStats, LookupStatus, TableRef,
};
use crate::policy::TableLookupPolicy;

#[derive(Debug, Clone)]
pub struct BlobLookupOptions {
    pub table_cache_ttl: Duration,
    pub descriptor_cache_ttl: Duration,
    pub descriptor_cache_max_bytes: u64,
    pub descriptor_cache_max_entry_bytes: u64,
    pub global_index_thread_num: usize,
}

impl Default for BlobLookupOptions {
    fn default() -> Self {
        Self {
            table_cache_ttl: Duration::from_secs(30),
            descriptor_cache_ttl: Duration::from_secs(60),
            descriptor_cache_max_bytes: 64 * 1024 * 1024,
            descriptor_cache_max_entry_bytes: 4 * 1024 * 1024,
            global_index_thread_num: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescriptorCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: u64,
    pub weighted_bytes: u64,
}

#[derive(Debug, Clone)]
struct CachedTable {
    table: Table,
    loaded_at: Instant,
}

#[derive(Debug, Default)]
struct TableCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Debug, Default)]
struct DescriptorCacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DescriptorCacheKey {
    table: TableRef,
    table_location: String,
    snapshot_id: i64,
    schema_id: i64,
    snapshot_fingerprint: Vec<u8>,
    request_fingerprint: Vec<u8>,
}

#[derive(Debug)]
struct CachedDescriptorResponse {
    response: BatchGetResponse,
    weight: u32,
}

type SharedLookupResult = Arc<std::result::Result<BatchGetResponse, Arc<LookupError>>>;

#[derive(Debug, Default)]
struct DescriptorLoad {
    result: Mutex<Option<SharedLookupResult>>,
    notify: Notify,
}

impl DescriptorLoad {
    fn complete(&self, result: SharedLookupResult) -> Result<()> {
        *self.result.lock().map_err(|_| cache_lock_error())? = Some(result);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn wait(&self) -> Result<SharedLookupResult> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.result.lock().map_err(|_| cache_lock_error())?.clone() {
                return Ok(result);
            }
            notified.await;
        }
    }
}

struct DescriptorLoadGuard {
    load: Arc<DescriptorLoad>,
    completed: bool,
}

impl DescriptorLoadGuard {
    fn new(load: Arc<DescriptorLoad>) -> Self {
        Self {
            load,
            completed: false,
        }
    }

    fn complete(mut self, result: SharedLookupResult) -> Result<()> {
        self.load.complete(result)?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for DescriptorLoadGuard {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self
                .load
                .complete(Arc::new(Err(Arc::new(LookupError::LoadCancelled))));
        }
    }
}

#[derive(Clone)]
pub struct BlobLookupService {
    catalog: Arc<dyn Catalog>,
    policies: Arc<BTreeMap<TableRef, TableLookupPolicy>>,
    options: BlobLookupOptions,
    table_cache: Arc<RwLock<HashMap<TableRef, CachedTable>>>,
    table_load_gates: Arc<Mutex<HashMap<TableRef, Arc<AsyncMutex<()>>>>>,
    table_cache_counters: Arc<TableCacheCounters>,
    descriptor_cache: Option<Cache<DescriptorCacheKey, Arc<CachedDescriptorResponse>>>,
    descriptor_loads: Arc<Mutex<HashMap<DescriptorCacheKey, Weak<DescriptorLoad>>>>,
    descriptor_cache_counters: Arc<DescriptorCacheCounters>,
}

impl std::fmt::Debug for BlobLookupService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobLookupService")
            .field("policies", &self.policies.keys().collect::<Vec<_>>())
            .field("options", &self.options)
            .field("table_cache_stats", &self.table_cache_stats())
            .field("descriptor_cache_stats", &self.descriptor_cache_stats())
            .finish_non_exhaustive()
    }
}

impl BlobLookupService {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        policies: impl IntoIterator<Item = TableLookupPolicy>,
    ) -> Result<Self> {
        Self::new_with_options(catalog, policies, BlobLookupOptions::default())
    }

    pub fn new_with_options(
        catalog: Arc<dyn Catalog>,
        policies: impl IntoIterator<Item = TableLookupPolicy>,
        options: BlobLookupOptions,
    ) -> Result<Self> {
        if options.global_index_thread_num == 0 {
            return Err(LookupError::InvalidPolicy(
                "global index thread count must be positive".to_string(),
            ));
        }
        let mut by_table = BTreeMap::new();
        for policy in policies {
            policy.validate_definition()?;
            let table = policy.table.clone();
            if by_table.insert(table.clone(), policy).is_some() {
                return Err(LookupError::InvalidPolicy(format!(
                    "duplicate policy for {}",
                    table.full_name()
                )));
            }
        }
        let descriptor_cache = if options.descriptor_cache_ttl.is_zero()
            || options.descriptor_cache_max_bytes == 0
            || options.descriptor_cache_max_entry_bytes == 0
        {
            None
        } else {
            Some(
                Cache::builder()
                    .max_capacity(options.descriptor_cache_max_bytes)
                    .time_to_live(options.descriptor_cache_ttl)
                    .weigher(
                        |_key: &DescriptorCacheKey, value: &Arc<CachedDescriptorResponse>| {
                            value.weight
                        },
                    )
                    .build(),
            )
        };
        Ok(Self {
            catalog,
            policies: Arc::new(by_table),
            options,
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            table_load_gates: Arc::new(Mutex::new(HashMap::new())),
            table_cache_counters: Arc::new(TableCacheCounters::default()),
            descriptor_cache,
            descriptor_loads: Arc::new(Mutex::new(HashMap::new())),
            descriptor_cache_counters: Arc::new(DescriptorCacheCounters::default()),
        })
    }

    pub fn table_cache_stats(&self) -> TableCacheStats {
        TableCacheStats {
            hits: self.table_cache_counters.hits.load(Ordering::Relaxed),
            misses: self.table_cache_counters.misses.load(Ordering::Relaxed),
            entries: self
                .table_cache
                .read()
                .map(|cache| {
                    cache
                        .values()
                        .filter(|cached| cached.loaded_at.elapsed() < self.options.table_cache_ttl)
                        .count()
                })
                .unwrap_or_default(),
        }
    }

    pub fn descriptor_cache_stats(&self) -> DescriptorCacheStats {
        match &self.descriptor_cache {
            Some(cache) => DescriptorCacheStats {
                hits: self.descriptor_cache_counters.hits.load(Ordering::Relaxed),
                misses: self
                    .descriptor_cache_counters
                    .misses
                    .load(Ordering::Relaxed),
                entries: cache.entry_count(),
                weighted_bytes: cache.weighted_size(),
            },
            None => DescriptorCacheStats::default(),
        }
    }

    /// Reload and validate every configured table. This intentionally bypasses
    /// the TTL so readiness checks also verify catalog connectivity.
    pub async fn check_ready(&self) -> Result<()> {
        const MAX_CONCURRENT_READINESS_CHECKS: usize = 8;
        futures::stream::iter(self.policies.values().cloned())
            .map(|policy| async move {
                let table = self.catalog.get_table(&policy.table.identifier()).await?;
                policy.validate_table(&table)?;
                self.cache_table(policy.table.clone(), table)?;
                Ok::<(), LookupError>(())
            })
            .buffer_unordered(MAX_CONCURRENT_READINESS_CHECKS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    pub async fn batch_get(&self, request: BatchGetRequest) -> Result<BatchGetResponse> {
        let policy = self.policies.get(&request.table).ok_or_else(|| {
            LookupError::InvalidRequest(format!(
                "no lookup policy is configured for {}",
                request.table.full_name()
            ))
        })?;
        self.batch_get_with_policy(policy, request).await
    }

    async fn batch_get_with_policy(
        &self,
        policy: &TableLookupPolicy,
        request: BatchGetRequest,
    ) -> Result<BatchGetResponse> {
        validate_request(policy, &request)?;
        let base = self.get_table(&request.table).await?;

        let snapshot_id = match request.snapshot_id {
            Some(snapshot_id) if snapshot_id > 0 => Some(snapshot_id),
            Some(snapshot_id) => {
                return Err(LookupError::InvalidRequest(format!(
                    "snapshotId must be positive, got {snapshot_id}"
                )))
            }
            None => base.snapshot_manager().get_latest_snapshot_id().await?,
        };

        let Some(snapshot_id) = snapshot_id else {
            policy.validate_table(&base)?;
            let prepared = prepare_keys(request.keys, &policy.key_fields, base.schema().fields())?;
            return Ok(not_found_response(
                request.table,
                None,
                base.schema().id(),
                prepared,
            ));
        };

        let table = base
            .copy_with_time_travel_strict(HashMap::from([
                ("scan.snapshot-id".to_string(), snapshot_id.to_string()),
                ("blob-as-descriptor".to_string(), "true".to_string()),
                ("global-index.search-mode".to_string(), "full".to_string()),
                (
                    "global-index.thread-num".to_string(),
                    self.options.global_index_thread_num.to_string(),
                ),
            ]))
            .await?;
        policy.validate_table_for_request(&table, &request.blob_fields)?;

        let cache = self.descriptor_cache.clone();
        let cache_key = descriptor_cache_key(&request, snapshot_id, &table)?;
        if let Some(cache) = &cache {
            if let Some(response) = cache.get(&cache_key).await {
                self.descriptor_cache_counters
                    .hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(as_cache_hit(response.response.clone()));
            }
        }

        let (load, leader) = self.descriptor_load(&cache_key)?;
        if !leader {
            if cache.is_some() {
                self.descriptor_cache_counters
                    .hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            return shared_lookup_result(load.wait().await?, true);
        }
        let guard = DescriptorLoadGuard::new(load);
        if cache.is_some() {
            self.descriptor_cache_counters
                .misses
                .fetch_add(1, Ordering::Relaxed);
        }
        match execute_snapshot_lookup(policy, request, snapshot_id, table).await {
            Ok(response) => {
                if let Some(cache) = cache {
                    let weight = descriptor_cache_weight(&cache_key, &response);
                    if u64::from(weight) <= self.options.descriptor_cache_max_entry_bytes {
                        cache
                            .insert(
                                cache_key,
                                Arc::new(CachedDescriptorResponse {
                                    response: response.clone(),
                                    weight,
                                }),
                            )
                            .await;
                    }
                }
                guard.complete(Arc::new(Ok(response.clone())))?;
                Ok(response)
            }
            Err(error) => {
                let shared_error = Arc::new(clone_lookup_error_for_follower(&error));
                guard.complete(Arc::new(Err(shared_error)))?;
                Err(error)
            }
        }
    }

    fn descriptor_load(
        &self,
        cache_key: &DescriptorCacheKey,
    ) -> Result<(Arc<DescriptorLoad>, bool)> {
        let mut loads = self
            .descriptor_loads
            .lock()
            .map_err(|_| cache_lock_error())?;
        loads.retain(|_, load| load.strong_count() > 0);
        if let Some(load) = loads.get(cache_key).and_then(Weak::upgrade) {
            return Ok((load, false));
        }
        let load = Arc::new(DescriptorLoad::default());
        loads.insert(cache_key.clone(), Arc::downgrade(&load));
        Ok((load, true))
    }

    async fn get_table(&self, table_ref: &TableRef) -> Result<Table> {
        if self.options.table_cache_ttl.is_zero() {
            self.table_cache_counters
                .misses
                .fetch_add(1, Ordering::Relaxed);
            return Ok(self.catalog.get_table(&table_ref.identifier()).await?);
        }
        if let Some(table) = self.cached_table(table_ref)? {
            self.table_cache_counters
                .hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(table);
        }

        // Collapse concurrent cold loads for the same table into one catalog
        // request. Different tables use different gates and can load in parallel.
        let gate = self.table_load_gate(table_ref)?;
        let _guard = gate.lock().await;
        if let Some(table) = self.cached_table(table_ref)? {
            self.table_cache_counters
                .hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(table);
        }

        self.table_cache_counters
            .misses
            .fetch_add(1, Ordering::Relaxed);
        let table = self.catalog.get_table(&table_ref.identifier()).await?;
        self.cache_table(table_ref.clone(), table.clone())?;
        Ok(table)
    }

    fn cached_table(&self, table_ref: &TableRef) -> Result<Option<Table>> {
        Ok(self
            .table_cache
            .read()
            .map_err(|_| cache_lock_error())?
            .get(table_ref)
            .filter(|cached| cached.loaded_at.elapsed() < self.options.table_cache_ttl)
            .map(|cached| cached.table.clone()))
    }

    fn table_load_gate(&self, table_ref: &TableRef) -> Result<Arc<AsyncMutex<()>>> {
        Ok(self
            .table_load_gates
            .lock()
            .map_err(|_| cache_lock_error())?
            .entry(table_ref.clone())
            .or_default()
            .clone())
    }

    fn cache_table(&self, table_ref: TableRef, table: Table) -> Result<()> {
        if self.options.table_cache_ttl.is_zero() {
            return Ok(());
        }
        self.table_cache
            .write()
            .map_err(|_| cache_lock_error())?
            .insert(
                table_ref,
                CachedTable {
                    table,
                    loaded_at: Instant::now(),
                },
            );
        Ok(())
    }
}

async fn execute_snapshot_lookup(
    policy: &TableLookupPolicy,
    request: BatchGetRequest,
    snapshot_id: i64,
    table: Table,
) -> Result<BatchGetResponse> {
    let prepared = prepare_keys(request.keys, &policy.key_fields, table.schema().fields())?;
    let predicate = build_batch_predicate(&prepared, &policy.key_fields, table.schema().fields())?;
    let mut projection = policy.key_fields.clone();
    projection.extend(request.blob_fields.iter().cloned());
    let projection_refs = projection.iter().map(String::as_str).collect::<Vec<_>>();

    let mut builder = table.new_read_builder();
    builder
        .with_projection(&projection_refs)?
        .with_filter(predicate);
    let (plan, trace) = builder.new_scan().plan_with_trace().await?;
    if trace.snapshot_id != Some(snapshot_id) {
        return Err(LookupError::SnapshotMismatch {
            expected: snapshot_id,
            actual: trace.snapshot_id,
        });
    }
    enforce_budget(policy, trace.final_files, trace.planned_data_file_bytes)?;

    let read = builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;
    let mut matches = HashMap::<NormalizedKey, MatchedRow>::new();
    while let Some(batch) = stream.try_next().await? {
        collect_batch(
            &batch,
            &prepared,
            &policy.key_fields,
            &request.blob_fields,
            table.schema().fields(),
            request.descriptor_format,
            &mut matches,
        )?;
    }

    Ok(BatchGetResponse {
        table: request.table,
        snapshot_id: Some(snapshot_id),
        schema_id: table.schema().id(),
        cache_hit: false,
        scan: LookupScanStats {
            planned_files: trace.final_files,
            planned_bytes: trace.planned_data_file_bytes,
        },
        results: build_results(prepared, matches),
    })
}

fn descriptor_cache_key(
    request: &BatchGetRequest,
    snapshot_id: i64,
    table: &Table,
) -> Result<DescriptorCacheKey> {
    let snapshot = table.travel_snapshot().ok_or_else(|| {
        LookupError::UnexpectedResult(format!(
            "snapshot {snapshot_id} was not resolved for descriptor lookup"
        ))
    })?;
    if snapshot.id() != snapshot_id {
        return Err(LookupError::SnapshotMismatch {
            expected: snapshot_id,
            actual: Some(snapshot.id()),
        });
    }
    let snapshot_fingerprint = serde_json::to_vec(&(
        snapshot.id(),
        snapshot.schema_id(),
        snapshot.base_manifest_list(),
        snapshot.delta_manifest_list(),
        snapshot.changelog_manifest_list(),
        snapshot.index_manifest(),
        snapshot.commit_user(),
        snapshot.commit_identifier(),
        snapshot.time_millis(),
        snapshot.statistics(),
        snapshot.next_row_id(),
    ))
    .map_err(|error| {
        LookupError::UnexpectedResult(format!("failed to encode snapshot cache identity: {error}"))
    })?;
    let request_fingerprint = serde_json::to_vec(&(
        &request.keys,
        &request.blob_fields,
        request.descriptor_format,
    ))
    .map_err(|error| {
        LookupError::UnexpectedResult(format!("failed to encode descriptor cache key: {error}"))
    })?;
    Ok(DescriptorCacheKey {
        table: request.table.clone(),
        table_location: table.location().to_string(),
        snapshot_id,
        schema_id: table.schema().id(),
        snapshot_fingerprint,
        request_fingerprint,
    })
}

fn as_cache_hit(mut response: BatchGetResponse) -> BatchGetResponse {
    response.cache_hit = true;
    response
}

fn shared_lookup_result(result: SharedLookupResult, cache_hit: bool) -> Result<BatchGetResponse> {
    match result.as_ref() {
        Ok(response) if cache_hit => Ok(as_cache_hit(response.clone())),
        Ok(response) => Ok(response.clone()),
        Err(error) => Err(LookupError::Shared(error.clone())),
    }
}

fn clone_lookup_error_for_follower(error: &LookupError) -> LookupError {
    match error {
        LookupError::InvalidRequest(message) => LookupError::InvalidRequest(message.clone()),
        LookupError::InvalidPolicy(message) => LookupError::InvalidPolicy(message.clone()),
        LookupError::UnsupportedKeyType { field, data_type } => LookupError::UnsupportedKeyType {
            field: field.clone(),
            data_type: data_type.clone(),
        },
        LookupError::InvalidKeyValue { field, message } => LookupError::InvalidKeyValue {
            field: field.clone(),
            message: message.clone(),
        },
        LookupError::QueryBudgetExceeded {
            files,
            bytes,
            max_files,
            max_bytes,
        } => LookupError::QueryBudgetExceeded {
            files: *files,
            bytes: *bytes,
            max_files: *max_files,
            max_bytes: *max_bytes,
        },
        LookupError::SnapshotMismatch { expected, actual } => LookupError::SnapshotMismatch {
            expected: *expected,
            actual: *actual,
        },
        LookupError::InvalidDescriptor { field, message } => LookupError::InvalidDescriptor {
            field: field.clone(),
            message: message.clone(),
        },
        LookupError::UnexpectedResult(message) => LookupError::UnexpectedResult(message.clone()),
        LookupError::LoadCancelled => LookupError::LoadCancelled,
        LookupError::Shared(error) => LookupError::Shared(error.clone()),
        LookupError::Paimon(paimon::Error::TableNotExist { full_name }) => {
            LookupError::Paimon(paimon::Error::TableNotExist {
                full_name: full_name.clone(),
            })
        }
        LookupError::Paimon(paimon::Error::DatabaseNotExist { database }) => {
            LookupError::Paimon(paimon::Error::DatabaseNotExist {
                database: database.clone(),
            })
        }
        LookupError::Paimon(paimon::Error::SnapshotNotExist { snapshot_id }) => {
            LookupError::Paimon(paimon::Error::SnapshotNotExist {
                snapshot_id: *snapshot_id,
            })
        }
        LookupError::Paimon(_) | LookupError::PaimonUnavailable => LookupError::PaimonUnavailable,
    }
}

fn descriptor_cache_weight(key: &DescriptorCacheKey, response: &BatchGetResponse) -> u32 {
    let response_bytes = serde_json::to_vec(response)
        .map(|value| value.len())
        .unwrap_or(usize::MAX);
    let total = response_bytes
        .saturating_add(key.request_fingerprint.len())
        .saturating_add(key.table.database.len())
        .saturating_add(key.table.table.len())
        .saturating_add(key.table_location.len())
        .saturating_add(key.snapshot_fingerprint.len());
    u32::try_from(total).unwrap_or(u32::MAX).max(1)
}

fn cache_lock_error() -> LookupError {
    LookupError::UnexpectedResult("lookup cache lock was poisoned".to_string())
}

#[derive(Debug)]
struct MatchedRow {
    count: usize,
    blobs: BTreeMap<String, Option<BlobDescriptorDto>>,
}

fn validate_request(policy: &TableLookupPolicy, request: &BatchGetRequest) -> Result<()> {
    if request.keys.is_empty() {
        return Err(LookupError::InvalidRequest(
            "keys must not be empty".to_string(),
        ));
    }
    if request.keys.len() > policy.budget.max_batch_keys {
        return Err(LookupError::InvalidRequest(format!(
            "batch has {} keys, maximum is {}",
            request.keys.len(),
            policy.budget.max_batch_keys
        )));
    }
    if request.blob_fields.is_empty() {
        return Err(LookupError::InvalidRequest(
            "blobFields must not be empty".to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for field in &request.blob_fields {
        if !seen.insert(field) {
            return Err(LookupError::InvalidRequest(format!(
                "blobFields contains duplicate field '{field}'"
            )));
        }
        if !policy.blob_fields.contains(field) {
            return Err(LookupError::InvalidRequest(format!(
                "BLOB field '{field}' is not allowed for {}",
                request.table.full_name()
            )));
        }
    }
    Ok(())
}

fn enforce_budget(policy: &TableLookupPolicy, files: usize, bytes: u64) -> Result<()> {
    if files > policy.budget.max_planned_files || bytes > policy.budget.max_planned_bytes {
        return Err(LookupError::QueryBudgetExceeded {
            files,
            bytes,
            max_files: policy.budget.max_planned_files,
            max_bytes: policy.budget.max_planned_bytes,
        });
    }
    Ok(())
}

fn collect_batch(
    batch: &RecordBatch,
    prepared: &[PreparedKey],
    key_fields: &[String],
    blob_fields: &[String],
    schema_fields: &[paimon::spec::DataField],
    format: DescriptorFormat,
    matches: &mut HashMap<NormalizedKey, MatchedRow>,
) -> Result<()> {
    let requested = prepared
        .iter()
        .map(|key| &key.normalized)
        .collect::<std::collections::HashSet<_>>();
    for row in 0..batch.num_rows() {
        let key = normalized_key_from_batch(batch, row, key_fields, schema_fields)?;
        if !requested.contains(&key) {
            return Err(LookupError::UnexpectedResult(
                "reader returned a row outside the requested key set".to_string(),
            ));
        }
        let blobs = decode_descriptors(batch, row, key_fields.len(), blob_fields, format)?;
        matches
            .entry(key)
            .and_modify(|matched| {
                matched.count += 1;
                matched.blobs.clear();
            })
            .or_insert(MatchedRow { count: 1, blobs });
    }
    Ok(())
}

fn decode_descriptors(
    batch: &RecordBatch,
    row: usize,
    blob_start: usize,
    blob_fields: &[String],
    format: DescriptorFormat,
) -> Result<BTreeMap<String, Option<BlobDescriptorDto>>> {
    blob_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let array = batch
                .column(blob_start + index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    LookupError::UnexpectedResult(format!(
                        "BLOB field '{field}' did not produce an Arrow BinaryArray"
                    ))
                })?;
            if array.is_null(row) {
                return Ok((field.clone(), None));
            }
            let raw = array.value(row);
            let descriptor = BlobDescriptor::deserialize(raw).map_err(|error| {
                LookupError::InvalidDescriptor {
                    field: field.clone(),
                    message: error.to_string(),
                }
            })?;
            descriptor
                .validate()
                .map_err(|error| LookupError::InvalidDescriptor {
                    field: field.clone(),
                    message: error.to_string(),
                })?;
            let encoded = (format == DescriptorFormat::PaimonBase64)
                .then(|| base64::engine::general_purpose::STANDARD.encode(raw));
            Ok((
                field.clone(),
                Some(BlobDescriptorDto {
                    version: descriptor.version(),
                    uri: descriptor.uri().to_string(),
                    offset: descriptor.offset(),
                    length: descriptor.length(),
                    encoded,
                }),
            ))
        })
        .collect()
}

fn build_results(
    prepared: Vec<PreparedKey>,
    matches: HashMap<NormalizedKey, MatchedRow>,
) -> Vec<LookupResult> {
    prepared
        .into_iter()
        .map(|key| match matches.get(&key.normalized) {
            None => LookupResult {
                key: key.original,
                status: LookupStatus::NotFound,
                blobs: BTreeMap::new(),
            },
            Some(matched) if matched.count == 1 => LookupResult {
                key: key.original,
                status: LookupStatus::Found,
                blobs: matched.blobs.clone(),
            },
            Some(_) => LookupResult {
                key: key.original,
                status: LookupStatus::NonUnique,
                blobs: BTreeMap::new(),
            },
        })
        .collect()
}

fn not_found_response(
    table: TableRef,
    snapshot_id: Option<i64>,
    schema_id: i64,
    prepared: Vec<PreparedKey>,
) -> BatchGetResponse {
    BatchGetResponse {
        table,
        snapshot_id,
        schema_id,
        cache_hit: false,
        scan: LookupScanStats::default(),
        results: prepared
            .into_iter()
            .map(|key| LookupResult {
                key: key.original,
                status: LookupStatus::NotFound,
                blobs: BTreeMap::new(),
            })
            .collect(),
    }
}
