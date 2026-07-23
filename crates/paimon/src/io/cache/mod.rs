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

mod disk;
mod file_type;
mod reader;

use self::file_type::FileType;
use crate::common::{CatalogOptions, Options};
use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use disk::{BlockKey, DiskCache};
pub(super) use reader::CachedFileReader;

const CACHE_DIRECTORY_NAME: &str = "paimon-local-cache-v2";
const DEFAULT_FILE_SIZE_CAPACITY: usize = 65_536;

#[derive(Debug)]
pub(crate) struct LocalCache {
    disk: DiskCache,
    namespace: String,
    block_size: u64,
    whitelist: HashSet<FileType>,
    file_sizes: Mutex<IndexMap<String, u64>>,
    file_size_capacity: usize,
    in_flight: tokio::sync::Mutex<HashMap<BlockKey, Weak<tokio::sync::Mutex<()>>>>,
    path_states: Mutex<HashMap<String, Weak<PathCacheState>>>,
}

#[derive(Debug)]
struct PathCacheState {
    generation: std::sync::atomic::AtomicU64,
    publish_gate: tokio::sync::RwLock<()>,
}

#[derive(Clone)]
pub(super) struct CacheReadToken {
    generation: u64,
    state: Arc<PathCacheState>,
}

impl LocalCache {
    pub(super) fn new(config: LocalCacheConfig) -> crate::Result<Self> {
        let file_size_capacity = config
            .max_size
            .map(|max_size| max_size / config.block_size)
            .and_then(|capacity| usize::try_from(capacity).ok())
            .unwrap_or(DEFAULT_FILE_SIZE_CAPACITY)
            .clamp(1, DEFAULT_FILE_SIZE_CAPACITY);
        Ok(Self {
            disk: DiskCache::new(config.dir.join(CACHE_DIRECTORY_NAME), config.max_size)?,
            namespace: config.namespace,
            block_size: config.block_size,
            whitelist: config.whitelist,
            file_sizes: Mutex::new(IndexMap::new()),
            file_size_capacity,
            in_flight: tokio::sync::Mutex::new(HashMap::new()),
            path_states: Mutex::new(HashMap::new()),
        })
    }

    fn block_size(&self) -> u64 {
        self.block_size
    }

    fn block_key(&self, path: &str, block_index: u64) -> BlockKey {
        BlockKey::with_namespace(&self.namespace, path, self.block_size, block_index)
    }

    pub(super) fn is_cacheable(&self, path: &str) -> bool {
        !FileType::is_mutable(path) && self.whitelist.contains(&FileType::classify(path))
    }

    async fn get_block(&self, key: &BlockKey, token: &CacheReadToken) -> Option<bytes::Bytes> {
        if token
            .state
            .generation
            .load(std::sync::atomic::Ordering::SeqCst)
            != token.generation
        {
            return None;
        }
        let payload = self.disk.get_block(key).await;
        if token
            .state
            .generation
            .load(std::sync::atomic::Ordering::SeqCst)
            == token.generation
        {
            payload
        } else {
            None
        }
    }

    async fn put_block(&self, key: &BlockKey, payload: bytes::Bytes, token: &CacheReadToken) {
        let _publish_guard = token.state.publish_gate.read().await;
        if token
            .state
            .generation
            .load(std::sync::atomic::Ordering::SeqCst)
            != token.generation
        {
            return;
        }
        self.disk.put_block(key, payload).await;
    }

    async fn remove_block(&self, key: &BlockKey) {
        self.disk.remove_block(key).await;
    }

    pub(super) fn read_token(&self, path: &str) -> CacheReadToken {
        let state = self.path_state(path);
        CacheReadToken {
            generation: state.generation.load(std::sync::atomic::Ordering::SeqCst),
            state,
        }
    }

    fn path_state(&self, path: &str) -> Arc<PathCacheState> {
        let mut states = self
            .path_states
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(state) = states.get(path).and_then(Weak::upgrade) {
            return state;
        }
        if states.len() >= 1024 {
            states.retain(|_, state| state.strong_count() > 0);
        }
        let state = Arc::new(PathCacheState {
            generation: std::sync::atomic::AtomicU64::new(0),
            publish_gate: tokio::sync::RwLock::new(()),
        });
        states.insert(path.to_string(), Arc::downgrade(&state));
        state
    }

    async fn block_load_lock(&self, key: &BlockKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut in_flight = self.in_flight.lock().await;
        if let Some(lock) = in_flight.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        if in_flight.len() >= 1024 {
            in_flight.retain(|_, lock| lock.strong_count() > 0);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        in_flight.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }

    async fn release_block_load_lock(&self, key: &BlockKey, lock: &Arc<tokio::sync::Mutex<()>>) {
        let mut in_flight = self.in_flight.lock().await;
        if Arc::strong_count(lock) == 1
            && in_flight
                .get(key)
                .and_then(Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, lock))
        {
            in_flight.remove(key);
        }
    }

    pub(super) fn file_size(&self, path: &str) -> Option<u64> {
        let mut file_sizes = self
            .file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let size = file_sizes.shift_remove(path)?;
        file_sizes.insert(path.to_string(), size);
        Some(size)
    }

    pub(super) fn put_file_size(&self, path: &str, size: u64) {
        let mut file_sizes = self
            .file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        file_sizes.shift_remove(path);
        file_sizes.insert(path.to_string(), size);
        while file_sizes.len() > self.file_size_capacity {
            file_sizes.shift_remove_index(0);
        }
    }

    pub(super) async fn invalidate_path(&self, path: &str) {
        let state = self.path_state(path);
        let _publish_guard = state.publish_gate.write().await;
        state
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shift_remove(path);
        self.disk.invalidate_path(&self.namespace, path).await;
    }

    pub(super) async fn invalidate_prefix(&self, prefix: &str) {
        let prefix = prefix.trim_end_matches('/');
        let states = {
            let states = self
                .path_states
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            states
                .iter()
                .filter(|(path, _)| path_matches_prefix(path, prefix))
                .filter_map(|(_, state)| Weak::upgrade(state))
                .collect::<Vec<_>>()
        };
        for state in states {
            let _publish_guard = state.publish_gate.write().await;
            state
                .generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|path, _| !path_matches_prefix(path, prefix));
        self.disk.invalidate_prefix(&self.namespace, prefix).await;
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn create_local_cache(options: &Options) -> crate::Result<Option<Arc<LocalCache>>> {
    LocalCacheConfig::from_options(options)?
        .map(LocalCache::new)
        .transpose()
        .map(|cache| cache.map(Arc::new))
}

#[derive(Debug)]
pub(crate) struct LocalCacheConfig {
    dir: PathBuf,
    namespace: String,
    max_size: Option<u64>,
    block_size: u64,
    whitelist: HashSet<FileType>,
}

impl LocalCacheConfig {
    pub(crate) fn from_options(options: &Options) -> crate::Result<Option<Self>> {
        let enabled = match options
            .get(CatalogOptions::LOCAL_CACHE_ENABLED)
            .map(|value| value.trim())
        {
            None => false,
            Some(value) if value.eq_ignore_ascii_case("true") => true,
            Some(value) if value.eq_ignore_ascii_case("false") => false,
            Some(value) => {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Invalid boolean for {}: '{}'",
                        CatalogOptions::LOCAL_CACHE_ENABLED,
                        value
                    ),
                });
            }
        };
        if !enabled {
            return Ok(None);
        }

        let dir = options
            .get(CatalogOptions::LOCAL_CACHE_DIR)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "Missing required option: {}",
                    CatalogOptions::LOCAL_CACHE_DIR
                ),
            })?
            .into();

        let max_size = options
            .get(CatalogOptions::LOCAL_CACHE_MAX_SIZE)
            .map(|value| parse_memory_size(CatalogOptions::LOCAL_CACHE_MAX_SIZE, value))
            .transpose()?;
        let block_size = options
            .get(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE)
            .map(|value| parse_memory_size(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, value))
            .transpose()?
            .unwrap_or(1024 * 1024);
        if block_size == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "{} must be greater than zero",
                    CatalogOptions::LOCAL_CACHE_BLOCK_SIZE
                ),
            });
        }
        let whitelist = options
            .get(CatalogOptions::LOCAL_CACHE_WHITELIST)
            .map(String::as_str)
            .unwrap_or("meta,global-index");

        Ok(Some(Self {
            dir,
            namespace: catalog_namespace(options),
            max_size,
            block_size,
            whitelist: FileType::parse_whitelist(whitelist),
        }))
    }
}

fn catalog_namespace(options: &Options) -> String {
    let mut entries = options
        .to_map()
        .iter()
        .filter(|(key, _)| !key.starts_with("local-cache."))
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut digest = Sha256::new();
    for (key, value) in entries {
        digest.update((key.len() as u64).to_le_bytes());
        digest.update(key.as_bytes());
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn parse_memory_size(key: &str, value: &str) -> crate::Result<u64> {
    let compact = value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let unit_start = compact
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(compact.len());
    let (number, unit) = compact.split_at(unit_start);
    let number = number
        .parse::<u64>()
        .map_err(|_| crate::Error::ConfigInvalid {
            message: format!("Invalid memory size for {key}: '{value}'"),
        })?;
    let multiplier = match unit {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Invalid memory size for {key}: '{value}'"),
            });
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| crate::Error::ConfigInvalid {
            message: format!("Memory size for {key} is too large: '{value}'"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_cache_config_disabled_by_default() {
        assert!(LocalCacheConfig::from_options(&Options::new())
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_local_cache_config_requires_directory_when_enabled() {
        let mut options = Options::new();
        options.set(crate::common::CatalogOptions::LOCAL_CACHE_ENABLED, "true");

        let error = LocalCacheConfig::from_options(&options).unwrap_err();
        assert!(matches!(error, crate::Error::ConfigInvalid { .. }));
        assert!(error.to_string().contains("local-cache.dir"));
    }

    #[test]
    fn test_local_cache_config_uses_disk_defaults() {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_DIR, "/tmp/paimon-cache");

        let config = LocalCacheConfig::from_options(&options).unwrap().unwrap();
        assert_eq!(config.dir, std::path::Path::new("/tmp/paimon-cache"));
        assert_eq!(config.max_size, None);
        assert_eq!(config.block_size, 1024 * 1024);
        assert_eq!(
            config.whitelist,
            std::collections::HashSet::from([FileType::Meta, FileType::GlobalIndex])
        );
    }

    #[test]
    fn test_local_cache_config_parses_custom_sizes() {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_DIR, "/tmp/paimon-cache");
        options.set(CatalogOptions::LOCAL_CACHE_MAX_SIZE, "2gb");
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "64 kb");
        options.set(CatalogOptions::LOCAL_CACHE_WHITELIST, "meta,data");

        let config = LocalCacheConfig::from_options(&options).unwrap().unwrap();
        assert_eq!(config.max_size, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(config.block_size, 64 * 1024);
        assert_eq!(
            config.whitelist,
            std::collections::HashSet::from([FileType::Meta, FileType::Data])
        );
    }

    #[test]
    fn test_local_cache_config_rejects_zero_block_size() {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_DIR, "/tmp/paimon-cache");
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "0");

        let error = LocalCacheConfig::from_options(&options).unwrap_err();
        assert!(matches!(error, crate::Error::ConfigInvalid { .. }));
        assert!(error.to_string().contains("local-cache.block-size"));
    }

    #[test]
    fn test_local_cache_config_rejects_invalid_enabled_value() {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "yes");

        let error = LocalCacheConfig::from_options(&options).unwrap_err();
        assert!(matches!(error, crate::Error::ConfigInvalid { .. }));
        assert!(error.to_string().contains("local-cache.enabled"));
    }

    #[tokio::test]
    async fn test_local_cache_file_size_is_removed_with_path_invalidation() {
        let directory = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(LocalCacheConfig {
            dir: directory.path().to_path_buf(),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";

        assert_eq!(cache.file_size(path), None);
        cache.put_file_size(path, 42);
        assert_eq!(cache.file_size(path), Some(42));
        cache.invalidate_path(path).await;
        assert_eq!(cache.file_size(path), None);
    }

    #[test]
    fn test_local_cache_uses_whitelist_and_bypasses_mutable_files() {
        let directory = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(LocalCacheConfig {
            dir: directory.path().to_path_buf(),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();

        assert!(cache.is_cacheable("s3://bucket/table/snapshot/snapshot-1"));
        assert!(!cache.is_cacheable("s3://bucket/table/data/data-1.parquet"));
        assert!(!cache.is_cacheable("s3://bucket/table/snapshot/LATEST"));
        assert!(!cache.is_cacheable("s3://bucket/table/tag/tag-production"));
    }

    #[test]
    fn test_local_cache_preserves_foreign_files_in_configured_directory() {
        let directory = tempfile::tempdir().unwrap();
        let foreign = directory.path().join("keep.txt");
        let foreign_temporary = directory.path().join("foreign.tmp.data");
        let nested_directory = directory.path().join("other-application");
        let nested = nested_directory.join("keep.bin");
        std::fs::create_dir(&nested_directory).unwrap();
        std::fs::write(&foreign, b"foreign").unwrap();
        std::fs::write(&foreign_temporary, b"foreign temporary").unwrap();
        std::fs::write(&nested, b"nested foreign").unwrap();

        LocalCache::new(LocalCacheConfig {
            dir: directory.path().to_path_buf(),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();

        assert_eq!(std::fs::read(foreign).unwrap(), b"foreign");
        assert_eq!(
            std::fs::read(foreign_temporary).unwrap(),
            b"foreign temporary"
        );
        assert_eq!(std::fs::read(nested).unwrap(), b"nested foreign");
    }

    #[test]
    fn test_local_cache_bounds_file_size_entries() {
        let directory = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(LocalCacheConfig {
            dir: directory.path().to_path_buf(),
            namespace: "test".to_string(),
            max_size: Some(8),
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();

        cache.put_file_size("snapshot-1", 1);
        cache.put_file_size("snapshot-2", 2);
        cache.put_file_size("snapshot-3", 3);

        assert_eq!(cache.file_size("snapshot-1"), None);
        assert_eq!(cache.file_size("snapshot-2"), Some(2));
        assert_eq!(cache.file_size("snapshot-3"), Some(3));
    }
}
