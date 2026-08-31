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
mod memory;
mod reader;
mod state;

use self::file_type::FileType;
use self::memory::MemoryCache;
use self::state::{BlockKey, CacheCoordinator, CacheReadToken};
use crate::common::{CatalogOptions, Options};
use crate::io::FileBlockCache;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use disk::DiskCache;
pub(super) use reader::CachedFileReader;

const CACHE_DIRECTORY_NAME: &str = "paimon-local-cache-v2";
const DEFAULT_FILE_SIZE_CAPACITY: usize = 65_536;

#[derive(Debug)]
pub(crate) struct LocalCache {
    backend: CacheBackend,
    coordinator: Arc<CacheCoordinator>,
    namespace: String,
    block_size: u64,
    whitelist: HashSet<FileType>,
    file_size_capacity: usize,
}

#[derive(Debug)]
enum CacheBackend {
    Memory(MemoryCache),
    Disk(Arc<DiskCache>),
    External(Arc<dyn FileBlockCache>),
}

impl LocalCache {
    pub(in crate::io) fn external(
        cache: Arc<dyn FileBlockCache>,
        block_size: u64,
        whitelist: &str,
    ) -> crate::Result<Self> {
        if block_size == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "External file cache block size must be greater than zero".to_string(),
            });
        }
        Ok(Self {
            backend: CacheBackend::External(cache),
            coordinator: Arc::new(CacheCoordinator::default()),
            namespace: String::new(),
            block_size,
            whitelist: FileType::parse_whitelist(whitelist),
            file_size_capacity: DEFAULT_FILE_SIZE_CAPACITY,
        })
    }

    pub(super) fn new(config: LocalCacheConfig) -> crate::Result<Self> {
        let file_size_capacity = config
            .max_size
            .map(|max_size| max_size / config.block_size)
            .and_then(|capacity| usize::try_from(capacity).ok())
            .unwrap_or(DEFAULT_FILE_SIZE_CAPACITY)
            .clamp(1, DEFAULT_FILE_SIZE_CAPACITY);
        let (backend, coordinator) = if let Some(dir) = config.dir {
            let disk = DiskCache::shared(dir.join(CACHE_DIRECTORY_NAME), config.max_size)?;
            let coordinator = disk.coordinator();
            (CacheBackend::Disk(disk), coordinator)
        } else {
            (
                CacheBackend::Memory(MemoryCache::new(config.max_size)),
                Arc::new(CacheCoordinator::default()),
            )
        };
        Ok(Self {
            backend,
            coordinator,
            namespace: config.namespace,
            block_size: config.block_size,
            whitelist: config.whitelist,
            file_size_capacity,
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

    async fn get_block(
        &self,
        key: &BlockKey,
        expected_len: usize,
        token: &CacheReadToken,
    ) -> Option<bytes::Bytes> {
        let _prefix_guard = self.coordinator.prefix_read_guard().await;
        let _publish_guard = token.publish_guard().await;
        if !token.is_current() {
            return None;
        }
        let payload = match &self.backend {
            CacheBackend::Memory(memory) => memory.get_block(key),
            CacheBackend::Disk(disk) => disk.get_block(key).await,
            CacheBackend::External(cache) => {
                let start = key.block_index.checked_mul(key.block_size)?;
                let length = u64::try_from(expected_len).ok()?;
                let end = start.checked_add(length)?;
                cache.get(&key.path, start..end).await
            }
        };
        if token.is_current() {
            payload
        } else {
            None
        }
    }

    async fn put_block(&self, key: &BlockKey, payload: bytes::Bytes, token: &CacheReadToken) {
        let _prefix_guard = self.coordinator.prefix_read_guard().await;
        let _publish_guard = token.publish_guard().await;
        if !token.is_current() {
            return;
        }
        match &self.backend {
            CacheBackend::Memory(memory) => memory.put_block(key, payload),
            CacheBackend::Disk(disk) => disk.put_block(key, payload).await,
            CacheBackend::External(cache) => {
                if let Some(offset) = key.block_index.checked_mul(key.block_size) {
                    cache.put(&key.path, offset, payload).await;
                }
            }
        }
    }

    async fn remove_block(&self, key: &BlockKey) {
        match &self.backend {
            CacheBackend::Memory(memory) => memory.remove_block(key),
            CacheBackend::Disk(disk) => disk.remove_block(key).await,
            CacheBackend::External(cache) => cache.invalidate_path(&key.path).await,
        }
    }

    pub(super) fn read_token(&self, path: &str) -> CacheReadToken {
        self.coordinator.read_token(&self.namespace, path)
    }

    async fn block_load_lock(&self, key: &BlockKey) -> Arc<tokio::sync::Mutex<()>> {
        self.coordinator.block_load_lock(key).await
    }

    async fn release_block_load_lock(&self, key: &BlockKey, lock: &Arc<tokio::sync::Mutex<()>>) {
        self.coordinator.release_block_load_lock(key, lock).await;
    }

    pub(super) async fn file_size(&self, path: &str, token: &CacheReadToken) -> Option<u64> {
        let _prefix_guard = self.coordinator.prefix_read_guard().await;
        let _publish_guard = token.publish_guard().await;
        token
            .is_current()
            .then(|| self.coordinator.file_size(&self.namespace, path))
            .flatten()
    }

    pub(super) async fn put_file_size(&self, path: &str, size: u64, token: &CacheReadToken) {
        let _prefix_guard = self.coordinator.prefix_read_guard().await;
        let _publish_guard = token.publish_guard().await;
        if !token.is_current() {
            return;
        }
        self.coordinator
            .put_file_size(&self.namespace, path, size, self.file_size_capacity);
    }

    pub(super) async fn invalidate_path(&self, path: &str) {
        let _guard = self
            .coordinator
            .begin_path_invalidation(&self.namespace, path)
            .await;
        match &self.backend {
            CacheBackend::Memory(memory) => memory.invalidate_path(&self.namespace, path),
            CacheBackend::Disk(disk) => disk.invalidate_path(&self.namespace, path).await,
            CacheBackend::External(cache) => cache.invalidate_path(path).await,
        }
    }

    pub(super) async fn invalidate_prefix(&self, prefix: &str) {
        if let CacheBackend::Disk(disk) = &self.backend {
            disk.ensure_recovered().await;
        }
        let _guard = self
            .coordinator
            .begin_prefix_invalidation(&self.namespace, prefix)
            .await;
        match &self.backend {
            CacheBackend::Memory(memory) => memory.invalidate_prefix(&self.namespace, prefix),
            CacheBackend::Disk(disk) => disk.invalidate_prefix(&self.namespace, prefix).await,
            CacheBackend::External(cache) => cache.invalidate_prefix(prefix).await,
        }
    }
}

pub(crate) fn create_local_cache(options: &Options) -> crate::Result<Option<Arc<LocalCache>>> {
    create_local_cache_with_namespace(options, options)
}

pub(crate) fn create_local_cache_with_namespace(
    cache_options: &Options,
    namespace_options: &Options,
) -> crate::Result<Option<Arc<LocalCache>>> {
    LocalCacheConfig::from_options_with_namespace(cache_options, namespace_options)?
        .map(LocalCache::new)
        .transpose()
        .map(|cache| cache.map(Arc::new))
}

#[derive(Debug)]
pub(crate) struct LocalCacheConfig {
    dir: Option<PathBuf>,
    namespace: String,
    max_size: Option<u64>,
    block_size: u64,
    whitelist: HashSet<FileType>,
}

impl LocalCacheConfig {
    #[cfg(test)]
    pub(crate) fn from_options(options: &Options) -> crate::Result<Option<Self>> {
        Self::from_options_with_namespace(options, options)
    }

    fn from_options_with_namespace(
        options: &Options,
        namespace_options: &Options,
    ) -> crate::Result<Option<Self>> {
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
            .map(PathBuf::from);

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
            namespace: catalog_namespace(namespace_options),
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
    crate::common::options::parse_memory_size(value)
        .map(|size| size as u64)
        .map_err(|error| crate::Error::ConfigInvalid {
            message: match error {
                crate::common::options::ParseMemorySizeError::Invalid => {
                    format!("Invalid memory size for {key}: '{value}'")
                }
                crate::common::options::ParseMemorySizeError::Overflow => {
                    format!("Memory size for {key} is too large: '{value}'")
                }
            },
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
    fn test_local_cache_config_uses_memory_when_enabled_without_directory() {
        let mut options = Options::new();
        options.set(crate::common::CatalogOptions::LOCAL_CACHE_ENABLED, "true");

        let config = LocalCacheConfig::from_options(&options).unwrap().unwrap();

        assert_eq!(config.dir, None);
        assert_eq!(config.max_size, None);
        assert_eq!(config.block_size, 1024 * 1024);
        assert_eq!(
            config.whitelist,
            HashSet::from([FileType::Meta, FileType::GlobalIndex])
        );
    }

    #[test]
    fn test_local_cache_config_uses_disk_defaults() {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_DIR, "/tmp/paimon-cache");

        let config = LocalCacheConfig::from_options(&options).unwrap().unwrap();
        assert_eq!(
            config.dir.as_deref(),
            Some(std::path::Path::new("/tmp/paimon-cache"))
        );
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
    fn test_local_cache_namespace_uses_effective_catalog_options() {
        let mut local_options = Options::new();
        local_options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        local_options.set(CatalogOptions::LOCAL_CACHE_DIR, "/tmp/paimon-cache");
        local_options.set(CatalogOptions::WAREHOUSE, "warehouse");
        let mut effective_options = local_options.clone();
        effective_options.set("s3.endpoint", "https://server-provided-endpoint");

        let local_namespace = LocalCacheConfig::from_options(&local_options)
            .unwrap()
            .unwrap()
            .namespace;
        let effective_config =
            LocalCacheConfig::from_options_with_namespace(&local_options, &effective_options)
                .unwrap()
                .unwrap();

        assert_eq!(
            effective_config.dir.as_deref(),
            Some(std::path::Path::new("/tmp/paimon-cache"))
        );
        assert_ne!(effective_config.namespace, local_namespace);
        assert_eq!(
            effective_config.namespace,
            catalog_namespace(&effective_options)
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
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let token = cache.read_token(path);

        assert_eq!(cache.file_size(path, &token).await, None);
        cache.put_file_size(path, 42, &token).await;
        assert_eq!(cache.file_size(path, &token).await, Some(42));
        cache.invalidate_path(path).await;
        assert_eq!(cache.file_size(path, &token).await, None);
    }

    #[tokio::test]
    async fn test_local_cache_file_size_is_invalidated_across_shared_instances() {
        let directory = tempfile::tempdir().unwrap();
        let config = || LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        };
        let first = LocalCache::new(config()).unwrap();
        let second = LocalCache::new(config()).unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let token = first.read_token(path);

        first.put_file_size(path, 42, &token).await;
        assert_eq!(second.file_size(path, &token).await, Some(42));
        second.invalidate_path(path).await;
        let current_token = first.read_token(path);
        assert_eq!(first.file_size(path, &current_token).await, None);
    }

    #[tokio::test]
    async fn test_stale_file_size_cannot_republish_after_invalidation() {
        let directory = tempfile::tempdir().unwrap();
        let config = || LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: None,
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        };
        let first = LocalCache::new(config()).unwrap();
        let second = LocalCache::new(config()).unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let stale_token = first.read_token(path);

        assert_eq!(first.file_size(path, &stale_token).await, None);
        second.invalidate_path(path).await;
        first.put_file_size(path, 42, &stale_token).await;

        let current_token = first.read_token(path);
        assert_eq!(first.file_size(path, &current_token).await, None);
    }

    #[test]
    fn test_local_cache_uses_whitelist_and_bypasses_mutable_files() {
        let directory = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
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
            dir: Some(directory.path().to_path_buf()),
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

    #[tokio::test]
    async fn test_local_cache_bounds_file_size_entries() {
        let directory = tempfile::tempdir().unwrap();
        let cache = LocalCache::new(LocalCacheConfig {
            dir: Some(directory.path().to_path_buf()),
            namespace: "test".to_string(),
            max_size: Some(8),
            block_size: 4,
            whitelist: HashSet::from([FileType::Meta]),
        })
        .unwrap();
        let first_token = cache.read_token("snapshot-1");
        let second_token = cache.read_token("snapshot-2");
        let third_token = cache.read_token("snapshot-3");

        cache.put_file_size("snapshot-1", 1, &first_token).await;
        cache.put_file_size("snapshot-2", 2, &second_token).await;
        cache.put_file_size("snapshot-3", 3, &third_token).await;

        assert_eq!(cache.file_size("snapshot-1", &first_token).await, None);
        assert_eq!(cache.file_size("snapshot-2", &second_token).await, Some(2));
        assert_eq!(cache.file_size("snapshot-3", &third_token).await, Some(3));
    }
}
