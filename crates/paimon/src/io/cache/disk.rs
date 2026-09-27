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

use bytes::Bytes;
use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::io::AsyncWriteExt;

use super::state::{BlockKey, CacheCoordinator, LogicalPath};

const CACHE_MAGIC: &[u8; 8] = b"PAIMONLC";
const CACHE_FORMAT_VERSION: u8 = 3;
const FIXED_HEADER_LEN: usize = CACHE_MAGIC.len() + 2 + 4 + 4 + 8 + 8 + 8;
const CHECKSUM_LEN: usize = 4;
const MAX_CACHE_KEY_HEADER_LEN: usize = 1024 * 1024;

impl BlockKey {
    pub(super) fn cache_relative_path(&self) -> PathBuf {
        let mut digest = Sha256::new();
        digest.update([CACHE_FORMAT_VERSION, u8::from(self.exact_range)]);
        digest.update((self.namespace.len() as u64).to_le_bytes());
        digest.update(self.namespace.as_bytes());
        digest.update((self.path.len() as u64).to_le_bytes());
        digest.update(self.path.as_bytes());
        digest.update(self.block_size.to_le_bytes());
        digest.update(self.block_index.to_le_bytes());
        let hex = hex::encode(digest.finalize());
        PathBuf::from(&hex[..2]).join(hex)
    }
}

#[derive(Debug)]
pub(super) struct BlockDecodeError(&'static str);

impl std::fmt::Display for BlockDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

#[derive(Debug)]
pub(super) struct DiskCache {
    root: PathBuf,
    state: Mutex<CacheState>,
    mutation: tokio::sync::Mutex<()>,
    recovered: tokio::sync::OnceCell<()>,
    coordinator: Arc<CacheCoordinator>,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: IndexMap<BlockKey, u64>,
    paths: HashMap<LogicalPath, HashSet<BlockKey>>,
    current_size: u64,
    max_size: Option<u64>,
}

impl DiskCache {
    #[cfg(test)]
    pub(super) fn new(root: impl AsRef<Path>, max_size: Option<u64>) -> crate::Result<Self> {
        let root = prepare_cache_root(root.as_ref())?;
        Ok(Self::new_resolved(root, max_size))
    }

    pub(super) fn shared(
        root: impl AsRef<Path>,
        max_size: Option<u64>,
    ) -> crate::Result<Arc<Self>> {
        let root = prepare_cache_root(root.as_ref())?;
        let mut registry = disk_cache_registry()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if registry.len() >= 1024 {
            registry.retain(|_, cache| cache.strong_count() > 0);
        }
        if let Some(cache) = registry.get(&root).and_then(Weak::upgrade) {
            cache.tighten_max_size(max_size);
            return Ok(cache);
        }

        let cache = Arc::new(Self::new_resolved(root.clone(), max_size));
        registry.insert(root, Arc::downgrade(&cache));
        Ok(cache)
    }

    fn new_resolved(root: PathBuf, max_size: Option<u64>) -> Self {
        Self {
            root,
            state: Mutex::new(CacheState {
                max_size,
                ..CacheState::default()
            }),
            mutation: tokio::sync::Mutex::new(()),
            recovered: tokio::sync::OnceCell::new(),
            coordinator: Arc::new(CacheCoordinator::default()),
        }
    }

    fn tighten_max_size(&self, requested: Option<u64>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.max_size = match (state.max_size, requested) {
            (None, requested) => requested,
            (current, None) => current,
            (Some(current), Some(requested)) => Some(current.min(requested)),
        };
    }

    pub(super) async fn ensure_recovered(&self) {
        self.recovered
            .get_or_init(|| async {
                let root = self.root.clone();
                let recovered = tokio::task::spawn_blocking(move || {
                    if let Err(error) = cleanup_temporary_files(&root) {
                        log::warn!(
                            "Failed to clean temporary files in local cache directory '{}': {error}",
                            root.display()
                        );
                    }
                    scan_existing_blocks(&root)
                })
                .await;
                match recovered {
                    Ok(recovered) => {
                        let mut state = self
                            .state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        for (key, encoded_size) in recovered.entries {
                            insert_state_entry(&mut state, key, encoded_size);
                        }
                    }
                    Err(error) => {
                        log::warn!(
                            "Failed to recover local cache directory '{}': {error}",
                            self.root.display()
                        );
                    }
                }
            })
            .await;
        self.evict_over_limit().await;
    }

    pub(super) fn coordinator(&self) -> Arc<CacheCoordinator> {
        self.coordinator.clone()
    }

    pub(super) fn can_store(&self, key: &BlockKey, payload_len: usize) -> bool {
        let Some(encoded_size) = encoded_block_disk_cost(key, payload_len) else {
            return false;
        };
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .max_size
            .is_none_or(|max_size| encoded_size <= max_size)
    }

    pub(super) async fn get_block(&self, key: &BlockKey) -> Option<Bytes> {
        self.ensure_recovered().await;
        if !self.is_active(key) {
            return None;
        }
        let path = self.root.join(key.cache_relative_path());
        let encoded = match tokio::fs::read(&path).await {
            Ok(encoded) => encoded,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.forget_entry(key);
                return None;
            }
            Err(error) => {
                log::debug!(
                    "Failed to read local cache block '{}': {error}",
                    path.display()
                );
                return None;
            }
        };
        match decode_owned_block(key, encoded) {
            Ok(payload) if self.touch_entry(key) => Some(payload),
            Ok(_) => None,
            Err(error) => {
                log::debug!(
                    "Discarding invalid local cache block '{}': {error}",
                    path.display()
                );
                self.forget_entry(key);
                let _ = tokio::fs::remove_file(path).await;
                None
            }
        }
    }

    pub(super) async fn put_block(&self, key: &BlockKey, payload: Bytes) {
        self.ensure_recovered().await;
        let Some(mut encoded_size) = encoded_block_disk_cost(key, payload.len()) else {
            return;
        };
        if key.exact_range && payload.len() > super::MAX_EXACT_RANGE_CACHE_ENTRY_SIZE {
            return;
        }
        if self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .max_size
            .is_some_and(|max_size| encoded_size > max_size)
        {
            return;
        }
        let Some(header) = encode_block_header(key, payload.len()) else {
            return;
        };

        let path = self.root.join(key.cache_relative_path());
        let Some(parent) = path.parent() else {
            return;
        };
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            log::debug!(
                "Failed to create local cache shard '{}': {error}",
                parent.display()
            );
            return;
        }

        let file_name = path
            .file_name()
            .map(|name| name.to_string_lossy())
            .unwrap_or_default();
        let temporary = parent.join(format!(".{file_name}.tmp.{}", uuid::Uuid::new_v4()));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut temporary_file = match options.open(&temporary).await {
            Ok(file) => file,
            Err(error) => {
                log::debug!(
                    "Failed to create local cache temporary block '{}': {error}",
                    temporary.display()
                );
                return;
            }
        };
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&header);
        checksum.update(&payload);
        let checksum = checksum.finalize().to_le_bytes();
        let write_result = async {
            temporary_file.write_all(&header).await?;
            temporary_file.write_all(&payload).await?;
            temporary_file.write_all(&checksum).await?;
            // Tokio may return from write_all before the blocking write completes.
            temporary_file.flush().await
        }
        .await;
        if let Err(error) = write_result {
            log::debug!(
                "Failed to write local cache temporary block '{}': {error}",
                temporary.display()
            );
            drop(temporary_file);
            let _ = tokio::fs::remove_file(&temporary).await;
            return;
        }
        if key.exact_range {
            if let Ok(metadata) = temporary_file.metadata().await {
                encoded_size = range_disk_cost(&metadata);
            }
            if self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .max_size
                .is_some_and(|limit| encoded_size > limit)
            {
                drop(temporary_file);
                let _ = tokio::fs::remove_file(&temporary).await;
                return;
            }
        }
        drop(temporary_file);
        let retired = {
            let _mutation = self.mutation.lock().await;
            if let Err(error) = tokio::fs::rename(&temporary, &path).await {
                log::debug!(
                    "Failed to publish local cache block '{}': {error}",
                    path.display()
                );
                let _ = tokio::fs::remove_file(temporary).await;
                return;
            }

            let to_evict = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state
                    .max_size
                    .is_some_and(|max_size| encoded_size > max_size)
                {
                    remove_state_entry(&mut state, key);
                    let mut to_evict = vec![key.clone()];
                    to_evict.extend(collect_evictions(&mut state));
                    to_evict
                } else {
                    insert_state_entry(&mut state, key.clone(), encoded_size);
                    collect_evictions(&mut state)
                }
            };
            self.retire_cache_files(to_evict).await
        };
        self.remove_cache_files(retired).await;
    }

    pub(super) async fn invalidate_path(&self, namespace: &str, path: &str) {
        self.ensure_recovered().await;
        self.invalidate_matching(|logical_path| {
            logical_path.namespace == namespace && logical_path.path == path
        })
        .await;
    }

    pub(super) async fn remove_block(&self, key: &BlockKey) {
        self.ensure_recovered().await;
        self.forget_entry(key);
        let path = self.root.join(key.cache_relative_path());
        if let Err(error) = tokio::fs::remove_file(&path).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                log::debug!(
                    "Failed to remove local cache block '{}': {error}",
                    path.display()
                );
            }
        }
    }

    pub(super) async fn invalidate_prefix(&self, namespace: &str, prefix: &str) {
        self.ensure_recovered().await;
        let prefix = prefix.trim_end_matches('/');
        self.invalidate_matching(|path| logical_path_matches_prefix(path, namespace, prefix))
            .await;
    }

    async fn invalidate_matching(&self, matches: impl Fn(&LogicalPath) -> bool) {
        let keys = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let keys = state
                .paths
                .keys()
                .filter(|path| matches(path))
                .filter_map(|path| state.paths.get(path))
                .flat_map(|keys| keys.iter().cloned())
                .collect::<Vec<_>>();
            for key in &keys {
                remove_state_entry(&mut state, key);
            }
            keys
        };
        for key in keys {
            let cache_path = self.root.join(key.cache_relative_path());
            if let Err(error) = tokio::fs::remove_file(&cache_path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    log::debug!(
                        "Failed to invalidate local cache block '{}': {error}",
                        cache_path.display()
                    );
                }
            }
        }
    }

    async fn evict_over_limit(&self) {
        let retired = {
            let _mutation = self.mutation.lock().await;
            let to_evict = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                collect_evictions(&mut state)
            };
            self.retire_cache_files(to_evict).await
        };
        self.remove_cache_files(retired).await;
    }

    async fn retire_cache_files(&self, keys: Vec<BlockKey>) -> Vec<PathBuf> {
        let mut retired = Vec::with_capacity(keys.len());
        for key in keys {
            let path = self.root.join(key.cache_relative_path());
            let Some(parent) = path.parent() else {
                continue;
            };
            let file_name = path
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default();
            let retired_path = parent.join(format!(".{file_name}.tmp.{}", uuid::Uuid::new_v4()));
            match tokio::fs::rename(&path, &retired_path).await {
                Ok(()) => retired.push(retired_path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    log::debug!(
                        "Failed to retire local cache block '{}': {error}",
                        path.display()
                    );
                }
            }
        }
        retired
    }

    async fn remove_cache_files(&self, paths: Vec<PathBuf>) {
        for path in paths {
            if let Err(error) = tokio::fs::remove_file(&path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    log::debug!(
                        "Failed to remove retired local cache block '{}': {error}",
                        path.display()
                    );
                }
            }
        }
    }

    fn is_active(&self, key: &BlockKey) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .contains_key(key)
    }

    fn touch_entry(&self, key: &BlockKey) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(encoded_size) = state.entries.shift_remove(key) else {
            return false;
        };
        state.entries.insert(key.clone(), encoded_size);
        true
    }

    fn forget_entry(&self, key: &BlockKey) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        remove_state_entry(&mut state, key);
    }
}

fn disk_cache_registry() -> &'static Mutex<HashMap<PathBuf, Weak<DiskCache>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<DiskCache>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn prepare_cache_root(root: &Path) -> crate::Result<PathBuf> {
    initialize_cache_directory(root)?;
    std::fs::canonicalize(root).map_err(|error| crate::Error::ConfigInvalid {
        message: format!(
            "Failed to resolve local cache directory '{}': {error}",
            root.display()
        ),
    })
}

fn collect_evictions(state: &mut CacheState) -> Vec<BlockKey> {
    let mut to_evict = Vec::new();
    {
        while state.entries.len() > super::MAX_CACHE_ENTRIES
            || state
                .max_size
                .is_some_and(|limit| state.current_size > limit)
        {
            let Some((eldest, size)) = state.entries.shift_remove_index(0) else {
                break;
            };
            state.current_size = state.current_size.saturating_sub(size);
            remove_path_index_entry(state, &eldest);
            to_evict.push(eldest);
        }
    }
    to_evict
}

fn logical_path(key: &BlockKey) -> LogicalPath {
    LogicalPath::from_key(key)
}

fn logical_path_matches_prefix(path: &LogicalPath, namespace: &str, prefix: &str) -> bool {
    path.namespace == namespace
        && (path.path == prefix
            || path
                .path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

fn insert_state_entry(state: &mut CacheState, key: BlockKey, encoded_size: u64) {
    if let Some(previous_size) = state.entries.shift_remove(&key) {
        state.current_size = state.current_size.saturating_sub(previous_size);
    }
    state
        .paths
        .entry(logical_path(&key))
        .or_default()
        .insert(key.clone());
    state.entries.insert(key, encoded_size);
    state.current_size = state.current_size.saturating_add(encoded_size);
}

fn remove_state_entry(state: &mut CacheState, key: &BlockKey) {
    if let Some(encoded_size) = state.entries.shift_remove(key) {
        state.current_size = state.current_size.saturating_sub(encoded_size);
        remove_path_index_entry(state, key);
    }
}

fn remove_path_index_entry(state: &mut CacheState, key: &BlockKey) {
    let path = logical_path(key);
    if let Some(keys) = state.paths.get_mut(&path) {
        keys.remove(key);
        if keys.is_empty() {
            state.paths.remove(&path);
        }
    }
}

fn initialize_cache_directory(root: &Path) -> crate::Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(root) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Local cache path '{}' must be a directory and not a symlink",
                    root.display()
                ),
            });
        }
    } else {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(root)
            .map_err(|error| crate::Error::ConfigInvalid {
                message: format!(
                    "Failed to initialize local cache directory '{}': {error}",
                    root.display()
                ),
            })?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| crate::Error::ConfigInvalid {
                message: format!(
                    "Failed to secure local cache directory '{}': {error}",
                    root.display()
                ),
            },
        )?;
    }
    Ok(())
}

fn cleanup_temporary_files(directory: &Path) -> std::io::Result<()> {
    for shard in std::fs::read_dir(directory)? {
        let shard = shard?;
        let shard_name = shard.file_name();
        let shard_name = shard_name.to_string_lossy();
        if !shard.file_type()?.is_dir() || !is_lower_hex(&shard_name, 2) {
            continue;
        }
        for entry in std::fs::read_dir(shard.path())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.file_type()?.is_file() && is_cache_temporary_name(&name, &shard_name) {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}

fn scan_existing_blocks(root: &Path) -> CacheState {
    let mut discovered = Vec::new();
    collect_cache_files(root, &mut discovered);
    discovered.sort_by_key(|(modified, _, _, _)| *modified);

    let mut state = CacheState::default();
    for (_, path, key, encoded_size) in discovered {
        if root.join(key.cache_relative_path()) != path {
            let _ = std::fs::remove_file(path);
            continue;
        }
        insert_state_entry(&mut state, key, encoded_size);
    }

    state
}

fn collect_cache_files(
    directory: &Path,
    discovered: &mut Vec<(std::time::SystemTime, PathBuf, BlockKey, u64)>,
) {
    let shards = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            log::debug!(
                "Failed to scan local cache directory '{}': {error}",
                directory.display()
            );
            return;
        }
    };
    for shard in shards.flatten() {
        let shard_name = shard.file_name();
        let shard_name = shard_name.to_string_lossy();
        let file_type = match shard.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_dir() || !is_lower_hex(&shard_name, 2) {
            continue;
        }
        let entries = match std::fs::read_dir(shard.path()) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !entry.file_type().is_ok_and(|file_type| file_type.is_file())
                || !is_cache_block_name(&name, &shard_name)
            {
                continue;
            }
            collect_cache_file(&entry, discovered);
        }
    }
}

fn collect_cache_file(
    entry: &std::fs::DirEntry,
    discovered: &mut Vec<(std::time::SystemTime, PathBuf, BlockKey, u64)>,
) {
    let path = entry.path();
    let metadata = match entry.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return,
    };
    let key = match read_block_key_header(&path, metadata.len()) {
        Ok(key) => key,
        Err(error) => {
            log::debug!(
                "Discarding invalid local cache block '{}' during startup: {error}",
                path.display()
            );
            let _ = std::fs::remove_file(path);
            return;
        }
    };
    let cost = if key.exact_range {
        range_disk_cost(&metadata)
    } else {
        metadata.len()
    };
    discovered.push((
        metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
        path,
        key,
        cost,
    ));
}

fn range_disk_cost(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata
            .len()
            .max(metadata.blocks().saturating_mul(512))
            .max(4096)
    }
    #[cfg(not(unix))]
    {
        metadata.len().saturating_add(4095) / 4096 * 4096
    }
}

fn read_block_key_header(path: &Path, encoded_len: u64) -> Result<BlockKey, BlockDecodeError> {
    let mut file =
        std::fs::File::open(path).map_err(|_| BlockDecodeError("cache block cannot be opened"))?;
    let mut fixed_header = [0_u8; FIXED_HEADER_LEN];
    file.read_exact(&mut fixed_header)
        .map_err(|_| BlockDecodeError("cache block header is truncated"))?;
    let layout = decode_block_layout(&fixed_header, encoded_len)?;
    let mut header = Vec::with_capacity(layout.header_len);
    header.extend_from_slice(&fixed_header);
    header.resize(layout.header_len, 0);
    file.read_exact(&mut header[FIXED_HEADER_LEN..])
        .map_err(|_| BlockDecodeError("cache block key header is truncated"))?;
    decode_block_header(&header, encoded_len).map(|(key, _)| key)
}

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_cache_block_name(name: &str, shard: &str) -> bool {
    is_lower_hex(name, 64) && name.starts_with(shard)
}

fn is_cache_temporary_name(name: &str, shard: &str) -> bool {
    let Some(name) = name.strip_prefix('.') else {
        return false;
    };
    let Some((digest, suffix)) = name.split_once(".tmp.") else {
        return false;
    };
    is_cache_block_name(digest, shard) && uuid::Uuid::parse_str(suffix).is_ok()
}

fn encoded_block_len(key: &BlockKey, payload_len: usize) -> Option<usize> {
    let key_len = key.namespace.len().checked_add(key.path.len())?;
    if key_len > MAX_CACHE_KEY_HEADER_LEN
        || u32::try_from(key.namespace.len()).is_err()
        || u32::try_from(key.path.len()).is_err()
    {
        return None;
    }
    FIXED_HEADER_LEN
        .checked_add(key_len)?
        .checked_add(payload_len)?
        .checked_add(CHECKSUM_LEN)
}

fn encoded_block_disk_cost(key: &BlockKey, payload_len: usize) -> Option<u64> {
    let encoded_len = u64::try_from(encoded_block_len(key, payload_len)?).ok()?;
    if key.exact_range {
        encoded_len
            .checked_add(4095)
            .map(|length| length / 4096 * 4096)
    } else {
        Some(encoded_len)
    }
}

fn encode_block_header(key: &BlockKey, payload_len: usize) -> Option<Vec<u8>> {
    let namespace = key.namespace.as_bytes();
    let path = key.path.as_bytes();
    let encoded_len = encoded_block_len(key, payload_len)?;
    let header_len = FIXED_HEADER_LEN
        .checked_add(namespace.len())?
        .checked_add(path.len())?;
    let mut header = Vec::with_capacity(header_len);
    header.extend_from_slice(CACHE_MAGIC);
    header.push(CACHE_FORMAT_VERSION);
    header.push(u8::from(key.exact_range));
    header.extend_from_slice(&u32::try_from(namespace.len()).ok()?.to_le_bytes());
    header.extend_from_slice(&u32::try_from(path.len()).ok()?.to_le_bytes());
    header.extend_from_slice(&key.block_size.to_le_bytes());
    header.extend_from_slice(&key.block_index.to_le_bytes());
    header.extend_from_slice(&u64::try_from(payload_len).ok()?.to_le_bytes());
    header.extend_from_slice(namespace);
    header.extend_from_slice(path);
    debug_assert_eq!(encoded_len, header.len() + payload_len + CHECKSUM_LEN);
    Some(header)
}

#[cfg(test)]
fn encode_block(key: &BlockKey, payload: &Bytes) -> Vec<u8> {
    let mut encoded = encode_block_header(key, payload.len()).unwrap();
    encoded.extend_from_slice(payload);
    let checksum = crc32fast::hash(&encoded);
    encoded.extend_from_slice(&checksum.to_le_bytes());
    encoded
}

#[cfg(test)]
fn decode_block(key: &BlockKey, encoded: &[u8]) -> Result<Bytes, BlockDecodeError> {
    decode_bytes_block(key, Bytes::copy_from_slice(encoded))
}

fn decode_owned_block(key: &BlockKey, encoded: Vec<u8>) -> Result<Bytes, BlockDecodeError> {
    decode_bytes_block(key, Bytes::from(encoded))
}

fn decode_bytes_block(key: &BlockKey, encoded: Bytes) -> Result<Bytes, BlockDecodeError> {
    let (decoded_key, payload) = decode_bytes_block_any(encoded)?;
    if &decoded_key != key {
        return Err(BlockDecodeError("cache block key does not match"));
    }
    Ok(payload)
}

fn decode_bytes_block_any(encoded: Bytes) -> Result<(BlockKey, Bytes), BlockDecodeError> {
    let (key, layout) = decode_block_header(&encoded, encoded.len() as u64)?;
    let payload_len = usize::try_from(layout.payload_len)
        .map_err(|_| BlockDecodeError("cache block payload is too large"))?;
    let payload_end = layout
        .header_len
        .checked_add(payload_len)
        .ok_or(BlockDecodeError("cache block length overflows"))?;
    let expected_checksum = u32::from_le_bytes(
        encoded[payload_end..payload_end + CHECKSUM_LEN]
            .try_into()
            .unwrap(),
    );
    if crc32fast::hash(&encoded[..payload_end]) != expected_checksum {
        return Err(BlockDecodeError("cache block checksum does not match"));
    }

    Ok((key, encoded.slice(layout.header_len..payload_end)))
}

#[derive(Clone, Copy, Debug)]
struct BlockLayout {
    exact_range: bool,
    namespace_len: usize,
    path_len: usize,
    block_size: u64,
    block_index: u64,
    payload_len: u64,
    header_len: usize,
}

fn decode_block_layout(
    fixed_header: &[u8],
    encoded_len: u64,
) -> Result<BlockLayout, BlockDecodeError> {
    if fixed_header.len() < FIXED_HEADER_LEN {
        return Err(BlockDecodeError("cache block header is truncated"));
    }
    if &fixed_header[..CACHE_MAGIC.len()] != CACHE_MAGIC {
        return Err(BlockDecodeError("cache block magic does not match"));
    }
    if fixed_header[CACHE_MAGIC.len()] != CACHE_FORMAT_VERSION {
        return Err(BlockDecodeError("cache block version is unsupported"));
    }

    let exact_range = match fixed_header[CACHE_MAGIC.len() + 1] {
        0 => false,
        1 => true,
        _ => return Err(BlockDecodeError("invalid cache range kind")),
    };
    let mut offset = CACHE_MAGIC.len() + 2;
    let namespace_len = read_u32(fixed_header, &mut offset)? as usize;
    let path_len = read_u32(fixed_header, &mut offset)? as usize;
    let block_size = read_u64(fixed_header, &mut offset)?;
    let block_index = read_u64(fixed_header, &mut offset)?;
    let payload_len = read_u64(fixed_header, &mut offset)?;
    let key_header_len = namespace_len
        .checked_add(path_len)
        .ok_or(BlockDecodeError("cache block key header length overflows"))?;
    if key_header_len > MAX_CACHE_KEY_HEADER_LEN {
        return Err(BlockDecodeError("cache block key header is too large"));
    }
    let header_len = offset
        .checked_add(namespace_len)
        .and_then(|length| length.checked_add(path_len))
        .ok_or(BlockDecodeError("cache block length overflows"))?;
    let expected_len = u64::try_from(header_len)
        .ok()
        .and_then(|length| length.checked_add(payload_len))
        .and_then(|length| length.checked_add(CHECKSUM_LEN as u64))
        .ok_or(BlockDecodeError("cache block length overflows"))?;
    if expected_len != encoded_len {
        return Err(BlockDecodeError("cache block length does not match"));
    }
    if block_size == 0 {
        return Err(BlockDecodeError("cache block size is zero"));
    }

    Ok(BlockLayout {
        exact_range,
        namespace_len,
        path_len,
        block_size,
        block_index,
        payload_len,
        header_len,
    })
}

fn decode_block_header(
    header: &[u8],
    encoded_len: u64,
) -> Result<(BlockKey, BlockLayout), BlockDecodeError> {
    let layout = decode_block_layout(header, encoded_len)?;
    if header.len() < layout.header_len {
        return Err(BlockDecodeError("cache block key header is truncated"));
    }

    let namespace_start = FIXED_HEADER_LEN;
    let namespace_end = namespace_start + layout.namespace_len;
    let namespace = std::str::from_utf8(&header[namespace_start..namespace_end])
        .map_err(|_| BlockDecodeError("cache block namespace is not UTF-8"))?;
    let path_end = namespace_end + layout.path_len;
    let path = std::str::from_utf8(&header[namespace_end..path_end])
        .map_err(|_| BlockDecodeError("cache block path is not UTF-8"))?;

    let mut key = BlockKey::with_namespace(namespace, path, layout.block_size, layout.block_index);
    key.exact_range = layout.exact_range;
    Ok((key, layout))
}

fn read_u32(encoded: &[u8], offset: &mut usize) -> Result<u32, BlockDecodeError> {
    let end = offset
        .checked_add(4)
        .ok_or(BlockDecodeError("cache block offset overflows"))?;
    let value = encoded
        .get(*offset..end)
        .ok_or(BlockDecodeError("cache block header is truncated"))?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, BlockDecodeError> {
    let end = offset
        .checked_add(8)
        .ok_or(BlockDecodeError("cache block offset overflows"))?;
    let value = encoded
        .get(*offset..end)
        .ok_or(BlockDecodeError("cache block header is truncated"))?;
    *offset = end;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_metadata_ranges_charge_disk_allocation_and_recover() {
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(directory.path(), Some(8192)).unwrap();
        let manifest = BlockKey::new("manifest", 8, 0);
        cache
            .put_block(&manifest, Bytes::from_static(b"manifest"))
            .await;
        let mut key = BlockKey::new("data.blob", 16384, 0);
        key.exact_range = true;
        cache.put_block(&key, Bytes::from(vec![0; 16384])).await;
        assert!(cache.get_block(&manifest).await.is_some());
        key.block_size = 1;
        for offset in 0..100 {
            key.block_index = offset;
            cache.put_block(&key, Bytes::from_static(b"x")).await;
        }
        {
            let state = cache.state.lock().unwrap();
            assert!(state.current_size <= 8192);
            assert!(state.entries.len() <= 2);
        }
        drop(cache);
        let cache = DiskCache::new(directory.path(), Some(8192)).unwrap();
        assert_eq!(cache.get_block(&key).await.unwrap(), b"x"[..]);
        let state = cache.state.lock().unwrap();
        assert!(state.current_size <= 8192);
        assert!(state.entries.len() <= 2);
    }

    #[test]
    fn test_disk_block_codec_round_trip() {
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 1024, 3);
        let payload = Bytes::from_static(b"cached block");

        let encoded = encode_block(&key, &payload);
        let decoded = decode_block(&key, &encoded).unwrap();

        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_disk_block_size_preflight_rejects_overflow() {
        let mut key = BlockKey::new("data.blob", 1, 0);
        key.exact_range = true;

        assert_eq!(encoded_block_disk_cost(&key, usize::MAX), None);
    }

    #[test]
    fn test_disk_block_codec_rejects_crc_corruption() {
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 1024, 3);
        let mut encoded = encode_block(&key, &Bytes::from_static(b"cached block"));
        let last_payload_byte = encoded.len() - CHECKSUM_LEN - 1;
        encoded[last_payload_byte] ^= 0xff;

        assert!(decode_block(&key, &encoded).is_err());
    }

    #[test]
    fn test_disk_block_key_uses_sharded_sha256_path() {
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 1024, 3);
        let relative = key.cache_relative_path();
        let components = relative
            .iter()
            .map(|component| component.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(components.len(), 2);
        assert_eq!(components[0].len(), 2);
        assert_eq!(components[1].len(), 64);
        assert!(components[1].starts_with(&components[0]));
        assert_ne!(
            relative,
            BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 1024, 4).cache_relative_path()
        );
    }

    #[tokio::test]
    async fn test_disk_cache_persistence_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 1024, 0);
        let payload = Bytes::from_static(b"persistent block");

        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&key, payload.clone()).await;
        // Check publication without scheduling another Tokio filesystem operation.
        assert_eq!(
            std::fs::read(directory.path().join(key.cache_relative_path())).unwrap(),
            encode_block(&key, &payload)
        );
        assert_eq!(cache.get_block(&key).await, Some(payload.clone()));
        drop(cache);

        let restarted = DiskCache::new(directory.path(), None).unwrap();
        assert_eq!(restarted.get_block(&key).await, Some(payload));
    }

    #[tokio::test]
    async fn test_disk_cache_shared_instances_enforce_one_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let first_key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let second_key = BlockKey::new("s3://bucket/table/snapshot/snapshot-2", 4, 0);
        let payload = Bytes::from_static(b"data");
        let one_block_size = encode_block(&first_key, &payload).len() as u64;

        let first = DiskCache::shared(directory.path(), None).unwrap();
        let second = DiskCache::shared(directory.path().join("."), Some(one_block_size)).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        first.put_block(&first_key, payload.clone()).await;
        second.put_block(&second_key, payload.clone()).await;
        assert_eq!(first.get_block(&first_key).await, None);
        assert_eq!(second.get_block(&second_key).await, Some(payload));
    }

    #[tokio::test]
    async fn test_disk_cache_restart_defers_crc_validation_until_first_hit() {
        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let block_path = directory.path().join(key.cache_relative_path());
        std::fs::create_dir_all(block_path.parent().unwrap()).unwrap();
        let mut encoded = encode_block(&key, &Bytes::from_static(b"data"));
        let payload_offset = encoded.len() - CHECKSUM_LEN - 1;
        encoded[payload_offset] ^= 0xff;
        std::fs::write(&block_path, encoded).unwrap();

        let restarted = DiskCache::new(directory.path(), None).unwrap();
        restarted.ensure_recovered().await;
        assert!(restarted.is_active(&key));

        assert_eq!(restarted.get_block(&key).await, None);
        assert!(!restarted.is_active(&key));
        assert!(!block_path.exists());
    }

    #[tokio::test]
    async fn test_disk_cache_does_not_read_unindexed_block_file() {
        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.ensure_recovered().await;
        let block_path = directory.path().join(key.cache_relative_path());
        std::fs::create_dir_all(block_path.parent().unwrap()).unwrap();
        std::fs::write(
            &block_path,
            encode_block(&key, &Bytes::from_static(b"data")),
        )
        .unwrap();

        assert_eq!(cache.get_block(&key).await, None);
        assert!(block_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_disk_cache_uses_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&key, Bytes::from_static(b"data")).await;

        assert_eq!(
            std::fs::metadata(directory.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(directory.path().join(key.cache_relative_path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_disk_cache_rejects_symlink_root() {
        let directory = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let symlink = directory.path().join("cache-link");
        std::os::unix::fs::symlink(target.path(), &symlink).unwrap();

        assert!(DiskCache::new(&symlink, None).is_err());
    }

    #[tokio::test]
    async fn test_disk_cache_persistence_removes_temporary_files_on_restart() {
        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let relative = key.cache_relative_path();
        let shard = directory.path().join(relative.parent().unwrap());
        std::fs::create_dir_all(&shard).unwrap();
        let digest = relative.file_name().unwrap().to_string_lossy();
        let temporary = shard.join(format!(".{digest}.tmp.{}", uuid::Uuid::nil()));
        std::fs::write(&temporary, b"partial").unwrap();

        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.ensure_recovered().await;

        assert!(!temporary.exists());
    }

    #[tokio::test]
    async fn test_disk_cache_lru_evicts_oldest_block_over_max_size() {
        let directory = tempfile::tempdir().unwrap();
        let first = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let second = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 1);
        let payload = Bytes::from_static(b"data");
        let one_block_size = encode_block(&first, &payload).len() as u64;
        let cache = DiskCache::new(directory.path(), Some(one_block_size)).unwrap();

        cache.put_block(&first, payload.clone()).await;
        cache.put_block(&second, payload.clone()).await;

        assert_eq!(cache.get_block(&first).await, None);
        assert_eq!(cache.get_block(&second).await, Some(payload));
    }

    #[tokio::test]
    async fn test_disk_cache_eviction_does_not_delete_republished_block() {
        let directory = tempfile::tempdir().unwrap();
        let key = BlockKey::new("s3://bucket/table/data/data.blob", 4, 0);
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&key, Bytes::from_static(b"old")).await;

        let retired = {
            let _mutation = cache.mutation.lock().await;
            let to_evict = {
                let mut state = cache
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                remove_state_entry(&mut state, &key);
                vec![key.clone()]
            };
            cache.retire_cache_files(to_evict).await
        };

        cache.put_block(&key, Bytes::from_static(b"new")).await;
        cache.remove_cache_files(retired).await;

        assert_eq!(
            cache.get_block(&key).await,
            Some(Bytes::from_static(b"new"))
        );
    }

    #[tokio::test]
    async fn test_disk_cache_lru_hit_refreshes_access_order() {
        let directory = tempfile::tempdir().unwrap();
        let first = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let second = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 1);
        let third = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 2);
        let payload = Bytes::from_static(b"data");
        let two_blocks = 2 * encode_block(&first, &payload).len() as u64;
        let cache = DiskCache::new(directory.path(), Some(two_blocks)).unwrap();

        cache.put_block(&first, payload.clone()).await;
        cache.put_block(&second, payload.clone()).await;
        assert_eq!(cache.get_block(&first).await, Some(payload.clone()));
        cache.put_block(&third, payload.clone()).await;

        assert_eq!(cache.get_block(&second).await, None);
        assert_eq!(cache.get_block(&first).await, Some(payload.clone()));
        assert_eq!(cache.get_block(&third).await, Some(payload));
    }

    #[tokio::test]
    async fn test_disk_cache_lru_evicts_over_limit_during_restart() {
        let directory = tempfile::tempdir().unwrap();
        let first = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let second = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 1);
        let payload = Bytes::from_static(b"data");
        let one_block_size = encode_block(&first, &payload).len() as u64;
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&first, payload.clone()).await;
        cache.put_block(&second, payload).await;
        drop(cache);

        let restarted = DiskCache::new(directory.path(), Some(one_block_size)).unwrap();
        restarted.ensure_recovered().await;

        let block_count = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|shard| std::fs::read_dir(shard.unwrap().path()).unwrap().count())
            .sum::<usize>();
        assert_eq!(block_count, 1);
    }

    #[tokio::test]
    async fn test_disk_cache_lru_invalidates_all_blocks_for_path() {
        let directory = tempfile::tempdir().unwrap();
        let path = "s3://bucket/table/snapshot/snapshot-1";
        let first = BlockKey::new(path, 4, 0);
        let second = BlockKey::new(path, 4, 1);
        let other = BlockKey::new("s3://bucket/table/snapshot/snapshot-2", 4, 0);
        let payload = Bytes::from_static(b"data");
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&first, payload.clone()).await;
        cache.put_block(&second, payload.clone()).await;
        cache.put_block(&other, payload.clone()).await;

        cache.invalidate_path("", path).await;

        assert_eq!(cache.get_block(&first).await, None);
        assert_eq!(cache.get_block(&second).await, None);
        assert_eq!(cache.get_block(&other).await, Some(payload));
    }

    #[tokio::test]
    async fn test_disk_cache_lru_invalidates_directory_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let inside = BlockKey::new("s3://bucket/table/snapshot/snapshot-1", 4, 0);
        let sibling = BlockKey::new("s3://bucket/table2/snapshot/snapshot-1", 4, 0);
        let payload = Bytes::from_static(b"data");
        let cache = DiskCache::new(directory.path(), None).unwrap();
        cache.put_block(&inside, payload.clone()).await;
        cache.put_block(&sibling, payload.clone()).await;

        cache.invalidate_prefix("", "s3://bucket/table").await;

        assert_eq!(cache.get_block(&inside).await, None);
        assert_eq!(cache.get_block(&sibling).await, Some(payload));
    }
}
