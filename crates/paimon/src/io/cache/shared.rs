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

//! Opt-in node-local cache. Block files, not a process-local index, are authoritative.
//! Epoch validation protects immutable hits; an exclusive directory lock serializes
//! publication, eviction, and invalidation. Block-striped locks coalesce downloads.

use super::*;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::time::Duration;

const MUTATION_MARKER: &str = "mutation-pending";

#[derive(Debug)]
pub(crate) struct SharedDiskCache {
    root: PathBuf,
    max_size: u64,
    high_priority_ratio: f64,
    staging_slots: Arc<tokio::sync::Semaphore>,
}

/// Unpublished inode, locked against cleanup and isolated from legacy temporaries.
struct StagedBlock {
    path: PathBuf,
    _file: File,
    len: u64,
}

impl StagedBlock {
    fn create(root: &Path, encoded: &[u8]) -> io::Result<Self> {
        let directory = root.join("staging");
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(uuid::Uuid::new_v4().to_string());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;
        let mut staged = Self {
            path,
            _file: file,
            len: encoded.len() as u64,
        };
        // Cleanup may reclaim the inode before we lock it.
        staged._file.try_lock().map_err(io::Error::other)?;
        if !staged.path.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "staging inode was reclaimed",
            ));
        }
        staged._file.write_all(encoded)?;
        Ok(staged)
    }
}

impl Drop for StagedBlock {
    fn drop(&mut self) {
        // Remove unpublished staging while still holding its lock.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn cleanup_staging(root: &Path) -> io::Result<()> {
    let entries = match std::fs::read_dir(root.join("staging")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_err() {
            continue;
        }
        match entry.file_type() {
            Ok(kind) if kind.is_file() => (),
            Ok(_) => continue,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
        let file = match OpenOptions::new().read(true).write(true).open(entry.path()) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        // Do not wait: the publisher may be waiting for our directory lock.
        match file.try_lock() {
            Ok(()) => match std::fs::remove_file(entry.path()) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            },
            Err(std::fs::TryLockError::WouldBlock) => (),
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
    }
    Ok(())
}

fn open_lock(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Try-lock polling avoids occupying a blocking thread while another process
/// performs remote I/O. Dropping the file (including cancellation/exit) releases it.
async fn lock(path: PathBuf) -> Option<File> {
    let file = tokio::task::spawn_blocking(move || open_lock(&path))
        .await
        .ok()?
        .ok()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match file.try_lock() {
            Ok(()) => return Some(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                if tokio::time::Instant::now() >= deadline {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => {
                log::debug!("Shared cache lock unavailable: {error}");
                return None;
            }
        }
    }
}

#[cfg(test)]
async fn read_lock(path: PathBuf) -> Option<File> {
    let file = tokio::task::spawn_blocking(move || open_lock(&path))
        .await
        .ok()?
        .ok()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match File::try_lock_shared(&file) {
            Ok(()) => return Some(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                if tokio::time::Instant::now() >= deadline {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => {
                log::debug!("Shared cache read lock unavailable: {error}");
                return None;
            }
        }
    }
}

/// Mutating storage after a cache invalidation timeout can leave cached data
/// reachable with no safe way to retry the mutation. Mutation fencing therefore
/// waits for the exclusive gate instead of falling back after the normal timeout.
async fn required_lock(path: PathBuf) -> io::Result<File> {
    let file = tokio::task::spawn_blocking(move || open_lock(&path))
        .await
        .map_err(io::Error::other)??;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
    }
}

#[derive(Debug)]
pub(in crate::io) struct SharedInvalidationGuard {
    root: PathBuf,
    gate: File,
    clear_marker: bool,
}

impl SharedInvalidationGuard {
    pub(in crate::io) async fn finish(self) {
        let Self {
            root,
            gate,
            clear_marker,
        } = self;
        let result = tokio::task::spawn_blocking(move || {
            let _gate = gate;
            finish_invalidation(&root)?;
            if clear_marker {
                std::fs::remove_file(root.join(MUTATION_MARKER))?;
            }
            Ok::<(), io::Error>(())
        })
        .await;
        // Cleanup failure leaves the mutation marker in place, so uncertain
        // blocks remain inaccessible.
        match result {
            Ok(Ok(())) => (),
            Ok(Err(error)) => log::debug!("Shared cache invalidation cleanup failed: {error}"),
            Err(error) => log::debug!("Shared cache invalidation task failed: {error}"),
        }
    }
}

impl SharedDiskCache {
    #[cfg(test)]
    pub(crate) fn new(root: PathBuf, max_size: u64) -> crate::Result<Self> {
        Self::new_with_ratio(root, max_size, DEFAULT_HIGH_PRIORITY_POOL_RATIO)
    }

    pub(crate) fn new_with_ratio(
        root: PathBuf,
        max_size: u64,
        high_priority_ratio: f64,
    ) -> crate::Result<Self> {
        let root = prepare_cache_root(&root)?;
        let initialize = || -> io::Result<()> {
            let gate = open_lock(&root.join("directory.lock"))?;
            gate.lock()?;
            let limit_path = root.join("capacity");
            match std::fs::read_to_string(&limit_path) {
                Ok(value) if value == max_size.to_string() => (),
                Ok(_) => {
                    return Err(io::Error::other(
                        "Shared cache participants must use the same local-cache.max-size",
                    ))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    std::fs::write(limit_path, max_size.to_string())?;
                }
                Err(e) => return Err(e),
            }
            let ratio_path = root.join("high-priority-pool-ratio");
            let ratio_bits = high_priority_ratio.to_bits().to_string();
            match std::fs::read_to_string(&ratio_path) {
                Ok(value) if value == ratio_bits => (),
                Ok(_) => {
                    return Err(io::Error::other(
                        "Shared cache participants must use the same local-cache.high-priority-pool-ratio",
                    ))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    std::fs::write(ratio_path, ratio_bits)?;
                }
                Err(e) => return Err(e),
            }
            if read_generation(&root).is_none() {
                // No block can be trusted without the epoch used to encode its
                // key. Clear unknown blocks before publishing a fresh epoch.
                std::fs::write(root.join("dirty"), b"")?;
                let used = recover_usage(&root, 0, high_priority_ratio)?;
                rotate_generation(&root)?;
                finish_usage(&root, used)?;
            }
            // Clean participants share persisted accounting; do not rescan every
            // block whenever another worker starts.
            if clean_usage(&root, max_size).is_none() {
                std::fs::write(root.join("dirty"), b"")?;
                let used = recover_usage(&root, max_size, high_priority_ratio)?;
                finish_usage(&root, used)?;
            }
            cleanup_staging(&root)?;
            Ok(())
        };
        initialize().map_err(|e| crate::Error::ConfigInvalid {
            message: e.to_string(),
        })?;
        Ok(Self {
            root,
            max_size,
            high_priority_ratio,
            staging_slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }

    pub(crate) fn generation(&self) -> Option<String> {
        let generation = read_generation(&self.root)?;
        mutation_cleared(&self.root).then_some(generation)
    }

    pub(crate) async fn load_lock(&self, key: &BlockKey) -> Option<File> {
        // Best-effort block coalescing; read_full locks only the first block.
        // Publication safety relies on the directory lock and generation checks.
        let stripe = key.cache_relative_path();
        lock(self.root.join(format!(
            "load-{}.lock",
            stripe.components().next()?.as_os_str().to_string_lossy()
        )))
        .await
    }

    pub(crate) async fn get(&self, key: &BlockKey, generation: &Option<String>) -> Option<Bytes> {
        if generation.is_none() {
            return None;
        }
        let root = self.root.clone();
        let key = generation_key(key, generation.as_ref()?);
        let generation = generation.clone();
        tokio::task::spawn_blocking(move || {
            if !mutation_cleared(&root)
                || std::fs::read_to_string(root.join("generation")).ok() != generation
            {
                return None;
            }
            let path = root.join(key.cache_relative_path());
            // Immutable inodes survive unlink; recheck the epoch after reading
            // to reject concurrent invalidation.
            let mut file = File::open(&path).ok()?;
            let metadata = file.metadata().ok()?;
            // Throttle recency writes for approximate cross-process LRU.
            if metadata
                .modified()
                .ok()
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age >= Duration::from_secs(10))
            {
                // Windows requires a write-capable handle to update mtime.
                // A failed touch must not turn a valid cached block into a miss.
                let _ = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .and_then(|file| file.set_modified(std::time::SystemTime::now()));
            }
            let mut data = Vec::with_capacity(usize::try_from(metadata.len()).ok()?);
            file.read_to_end(&mut data).ok()?;
            let payload = decode_block_owned(&key, data).ok()?;
            if !mutation_cleared(&root) || read_generation(&root) != generation {
                return None;
            }
            Some(payload)
        })
        .await
        .ok()
        .flatten()
    }

    pub(crate) async fn put(&self, key: &BlockKey, payload: Bytes, generation: &Option<String>) {
        if generation.is_none() {
            return;
        }
        let Some(staged) = self.stage_block(key, payload, generation).await else {
            return;
        };
        self.publish_staged(staged, generation).await;
    }

    async fn stage_block(
        &self,
        key: &BlockKey,
        payload: Bytes,
        generation: &Option<String>,
    ) -> Option<(BlockKey, StagedBlock, tokio::sync::OwnedSemaphorePermit)> {
        let root = self.root.clone();
        let key = generation_key(key, generation.as_ref()?);
        let max_size = self.max_size;
        // Stage outside the directory lock; recheck the generation before publishing.
        let slot = self.staging_slots.clone().acquire_owned().await.ok()?;
        let staging_root = root.clone();
        tokio::task::spawn_blocking(move || -> io::Result<_> {
            let encoded = encode_block(&key, &payload);
            if encoded.len() as u64 > max_size {
                return Err(io::Error::other("cache block exceeds capacity"));
            }
            let staged = StagedBlock::create(&staging_root, &encoded)?;
            Ok((key, staged, slot))
        })
        .await
        .ok()?
        .ok()
    }

    async fn publish_staged(
        &self,
        (key, staged, slot): (BlockKey, StagedBlock, tokio::sync::OwnedSemaphorePermit),
        generation: &Option<String>,
    ) {
        let root = self.root.clone();
        let generation = generation.clone();
        let max_size = self.max_size;
        let high_priority_ratio = self.high_priority_ratio;
        let Some(gate) = lock(self.root.join("directory.lock")).await else {
            return;
        };
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let _gate = gate;
            let _slot = slot;
            if !mutation_cleared(&root)
                || std::fs::read_to_string(root.join("generation")).ok() != generation
            {
                return Ok(());
            }
            let path = root.join(key.cache_relative_path());
            let mut used = if root.join("dirty").exists() {
                recover_usage(&root, max_size, high_priority_ratio)?
            } else {
                match std::fs::read_to_string(root.join("usage"))
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(used) => used,
                    None => recover_usage(&root, max_size, high_priority_ratio)?,
                }
            };
            // Keep the marker until publication and accounting both complete.
            // A killed process leaves it for the next writer to recover.
            std::fs::write(root.join("dirty"), b"")?;
            let mut previous = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if used.saturating_sub(previous).saturating_add(staged.len) > max_size {
                // Evict in batches, not one full directory scan per new block.
                used = recover_usage(
                    &root,
                    (max_size - staged.len).min(max_size - max_size / 10),
                    high_priority_ratio,
                )?;
                previous = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            }
            let parent = path.parent().expect("cache shard");
            std::fs::create_dir_all(parent)?;
            {
                std::fs::rename(&staged.path, &path)?;
            }
            finish_usage(
                &root,
                used.saturating_sub(previous).saturating_add(staged.len),
            )
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            log::debug!("Shared cache publication failed");
        }
    }

    pub(crate) async fn invalidate(&self) -> io::Result<()> {
        let gate = lock(self.root.join("directory.lock"))
            .await
            .ok_or_else(|| {
                io::Error::other("Shared cache invalidation could not acquire directory lock")
            })?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let _gate = gate;
            start_invalidation(&root)?;
            finish_invalidation(&root)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(in crate::io) async fn begin_invalidation(&self) -> io::Result<SharedInvalidationGuard> {
        let gate = required_lock(self.root.join("directory.lock")).await?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            // Unlike the accounting dirty marker, this survives cancellation.
            let clear_marker = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(MUTATION_MARKER))
            {
                Ok(_) => true,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
                Err(error) => return Err(error),
            };
            start_invalidation(&root)?;
            Ok(SharedInvalidationGuard {
                root,
                gate,
                clear_marker,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}

fn read_generation(root: &Path) -> Option<String> {
    let value = std::fs::read_to_string(root.join("generation")).ok()?;
    uuid::Uuid::parse_str(&value).ok()?;
    Some(value)
}

fn mutation_cleared(root: &Path) -> bool {
    matches!(
        std::fs::symlink_metadata(root.join(MUTATION_MARKER)),
        Err(error) if error.kind() == io::ErrorKind::NotFound
    )
}

// The mutation and accounting helpers below are only called while holding the
// exclusive directory lock.
fn rotate_generation(root: &Path) -> io::Result<()> {
    let temporary = root.join("generation.tmp");
    std::fs::write(&temporary, uuid::Uuid::new_v4().to_string())?;
    std::fs::rename(temporary, root.join("generation"))
}

fn start_invalidation(root: &Path) -> io::Result<()> {
    // Keep recovery enabled until block cleanup and accounting both complete.
    std::fs::write(root.join("dirty"), b"")?;
    // Epoch is part of each block's key. Rotation makes every old block
    // unreachable even if cleanup fails or this process exits midway.
    rotate_generation(root)
}

fn finish_invalidation(root: &Path) -> io::Result<()> {
    // This intentionally invalidates the whole shared directory.
    for (key, _) in scan_existing_blocks(root) {
        let _ = std::fs::remove_file(root.join(key.cache_relative_path()));
    }
    cleanup_temporary_files(root)?;
    cleanup_staging(root)?;
    finish_usage(root, existing_usage(root))
}

fn existing_usage(root: &Path) -> u64 {
    scan_existing_blocks(root)
        .iter()
        .fold(0u64, |used, (_, encoded_size)| {
            used.saturating_add(*encoded_size)
        })
}

fn clean_usage(root: &Path, max_size: u64) -> Option<u64> {
    if root.join("dirty").try_exists().ok()? {
        return None;
    }
    let used = std::fs::read_to_string(root.join("usage"))
        .ok()?
        .parse::<u64>()
        .ok()?;
    (used <= max_size).then_some(used)
}

fn recover_usage(root: &Path, limit: u64, high_priority_ratio: f64) -> io::Result<u64> {
    // Legacy temporaries are orphaned; staged files may still have live writers.
    cleanup_temporary_files(root)?;
    cleanup_staging(root)?;
    #[cfg(test)]
    RECOVERY_SCANS.with(|count| count.set(count.get() + 1));
    let mut state = CacheState::new(Some(limit), high_priority_ratio);
    let blocks = scan_existing_blocks(root);
    for (key, encoded_size) in blocks {
        insert_state_entry(&mut state, key, encoded_size);
    }
    for key in collect_evictions(&mut state) {
        std::fs::remove_file(root.join(key.cache_relative_path()))?;
    }
    Ok(state.current_size)
}

#[cfg(test)]
thread_local! {
    static RECOVERY_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn finish_usage(root: &Path, used: u64) -> io::Result<()> {
    std::fs::write(root.join("usage"), used.to_string())?;
    match std::fs::remove_file(root.join("dirty")) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn generation_key(key: &BlockKey, generation: &str) -> BlockKey {
    BlockKey::with_namespace(
        format!("{}:{generation}", key.namespace),
        &key.path,
        key.block_size,
        key.block_index,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileRead;
    use std::process::{Command, Stdio};

    const CHILD: &str = "io::cache::disk::shared::tests::shared_cache_child";

    #[derive(Debug)]
    struct Source(PathBuf);

    #[async_trait::async_trait]
    impl crate::io::FileRead for Source {
        async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.0)
                .unwrap()
                .write_all(b"read\n")
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Bytes::from_static(b"payload").slice(range.start as usize..range.end as usize))
        }
    }

    fn key() -> BlockKey {
        BlockKey::with_namespace("catalog", "data.parquet", 64, 0)
    }

    fn child(root: &Path, mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", CHILD, "--nocapture"])
            .env("PAIMON_SHARED_TEST_ROOT", root)
            .env("PAIMON_SHARED_TEST_MODE", mode)
            .stdin(Stdio::null());
        command
    }

    // A separate executable process, not another cache instance in this process.
    #[tokio::test]
    async fn shared_cache_child() {
        let Some(root) = std::env::var_os("PAIMON_SHARED_TEST_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let mode = std::env::var("PAIMON_SHARED_TEST_MODE").unwrap();
        if mode == "startup" {
            SharedDiskCache::new(root, 1024 * 1024).unwrap();
            assert_eq!(RECOVERY_SCANS.with(|count| count.get()), 0);
            return;
        }
        let cache = SharedDiskCache::new(root.clone(), 4096).unwrap();
        let token = cache.generation();
        match std::env::var("PAIMON_SHARED_TEST_MODE").unwrap().as_str() {
            "stage-hold" => {
                let _staged = StagedBlock::create(&root, b"staged").unwrap();
                std::fs::write(root.join("locked"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            "populate" => {
                let _guard = cache.load_lock(&key()).await.unwrap();
                if cache.get(&key(), &token).await.is_none() {
                    let mut source = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(root.join("source-reads"))
                        .unwrap();
                    source.write_all(b"read\n").unwrap();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    cache
                        .put(&key(), Bytes::from_static(b"payload"), &token)
                        .await;
                }
                assert_eq!(cache.get(&key(), &token).await.unwrap(), "payload");
            }
            "hold" => {
                let _guard = cache.load_lock(&key()).await.unwrap();
                std::fs::write(root.join("locked"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            "invalidate" => cache.invalidate().await.unwrap(),
            "reader" => {
                use crate::io::cache::{CachedFileReader, FileType, LocalCache, LocalCacheConfig};
                let local = Arc::new(
                    LocalCache::new(LocalCacheConfig {
                        shared: true,
                        dir: Some(root.clone()),
                        namespace: "catalog".into(),
                        max_size: Some(4096),
                        block_size: 64,
                        whitelist: std::collections::HashSet::from([FileType::StructuredData]),
                    })
                    .unwrap(),
                );
                let reader = CachedFileReader::new(
                    Arc::new(Source(root.join("source-reads"))),
                    "s3://bucket/table/data.parquet",
                    7,
                    local,
                );
                assert_eq!(reader.read(1..4).await.unwrap(), "ayl");
                assert_eq!(reader.read_full().await.unwrap(), "payload");
            }
            other => panic!("Unknown test mode: {other}"),
        }
    }

    #[test]
    fn processes_coalesce_downloads() {
        let directory = tempfile::tempdir().unwrap();
        let mut children: Vec<_> = (0..4)
            .map(|_| child(directory.path(), "populate").spawn().unwrap())
            .collect();
        for process in &mut children {
            assert!(process.wait().unwrap().success());
        }
        assert_eq!(
            std::fs::read_to_string(directory.path().join("source-reads")).unwrap(),
            "read\n"
        );
    }

    #[test]
    fn cached_file_reader_reuses_blocks_across_processes() {
        let directory = tempfile::tempdir().unwrap();
        let mut children: Vec<_> = (0..4)
            .map(|_| child(directory.path(), "reader").spawn().unwrap())
            .collect();
        for process in &mut children {
            assert!(process.wait().unwrap().success());
        }
        assert_eq!(
            std::fs::read_to_string(directory.path().join("source-reads")).unwrap(),
            "read\n"
        );
    }

    #[tokio::test]
    async fn another_process_invalidates_old_tokens() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let old = cache.generation();
        cache.put(&key(), Bytes::from_static(b"old"), &old).await;
        assert!(child(directory.path(), "invalidate")
            .status()
            .unwrap()
            .success());
        cache.put(&key(), Bytes::from_static(b"late"), &old).await;
        assert!(cache.get(&key(), &old).await.is_none());
        assert!(cache.get(&key(), &cache.generation()).await.is_none());
    }

    #[tokio::test]
    async fn mutation_does_not_issue_publishable_generation() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let before = cache.generation();

        let mutation = cache.begin_invalidation().await.unwrap();
        let during = cache.generation();
        assert!(during.is_none());
        mutation.finish().await;

        cache.put(&key(), Bytes::from_static(b"old"), &before).await;
        cache.put(&key(), Bytes::from_static(b"old"), &during).await;
        let after = cache.generation();
        assert_ne!(after, before);
        assert!(cache.get(&key(), &after).await.is_none());
        cache.put(&key(), Bytes::from_static(b"new"), &after).await;
        assert_eq!(cache.get(&key(), &after).await.unwrap(), "new");

        let cancelled = cache.begin_invalidation().await.unwrap();
        drop(cancelled);
        assert!(cache.generation().is_none());
        let uncertain = read_generation(directory.path());
        cache
            .put(&key(), Bytes::from_static(b"stale"), &uncertain)
            .await;
        assert!(cache.generation().is_none());
        assert!(cache.get(&key(), &uncertain).await.is_none());
        let reopened = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        assert!(reopened.generation().is_none());
        reopened.begin_invalidation().await.unwrap().finish().await;
        assert!(reopened.generation().is_none());
    }

    #[tokio::test]
    async fn staged_file_survives_recovery_and_publishes_after_gate_release() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let generation = cache.generation();
        let staged = cache
            .stage_block(&key(), Bytes::from_static(b"value"), &generation)
            .await
            .unwrap();
        let staged_path = staged.1.path.clone();
        let gate = open_lock(&directory.path().join("directory.lock")).unwrap();
        gate.lock().unwrap();
        assert_eq!(
            recover_usage(directory.path(), 4096, DEFAULT_HIGH_PRIORITY_POOL_RATIO).unwrap(),
            0
        );
        assert!(staged_path.exists());
        drop(gate);
        cache.publish_staged(staged, &generation).await;
        assert!(!staged_path.exists());
        assert_eq!(cache.get(&key(), &generation).await.unwrap(), "value");
        assert_eq!(
            std::fs::read_to_string(directory.path().join("usage"))
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            existing_usage(directory.path())
        );
    }

    #[test]
    fn process_exit_releases_staging_for_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let mut process = child(directory.path(), "stage-hold").spawn().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !directory.path().join("locked").exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let ready = directory.path().join("locked").exists();
        cleanup_staging(directory.path()).unwrap();
        let live = std::fs::read_dir(directory.path().join("staging"))
            .unwrap()
            .count();
        process.kill().unwrap();
        process.wait().unwrap();
        assert!(ready);
        assert_eq!(live, 1);
        SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        assert_eq!(
            std::fs::read_dir(directory.path().join("staging"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn dropping_unpublished_stage_removes_it() {
        let directory = tempfile::tempdir().unwrap();
        let staged = StagedBlock::create(directory.path(), b"value").unwrap();
        let path = staged.path.clone();
        assert!(path.exists());
        drop(staged);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn encoded_publication_cannot_cross_invalidation() {
        let directory = tempfile::tempdir().unwrap();
        let cache = std::sync::Arc::new(
            SharedDiskCache::new(directory.path().to_path_buf(), 4 * 1024 * 1024).unwrap(),
        );
        let old = cache.generation();
        let mutation = cache.begin_invalidation().await.unwrap();
        let writer = {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .put(&key(), Bytes::from(vec![42; 1024 * 1024]), &old)
                    .await;
            })
        };
        // Reject the old generation regardless of when encoding finishes.
        tokio::task::yield_now().await;
        mutation.finish().await;
        writer.await.unwrap();
        let current = cache.generation();
        assert!(cache.get(&key(), &current).await.is_none());
        cache
            .put(&key(), Bytes::from_static(b"current"), &current)
            .await;
        assert_eq!(cache.get(&key(), &current).await.unwrap(), "current");
    }

    #[tokio::test]
    async fn cache_hits_can_share_the_directory_read_lock() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let token = cache.generation();
        cache
            .put(&key(), Bytes::from_static(b"payload"), &token)
            .await;
        let gate = open_lock(&directory.path().join("directory.lock")).unwrap();
        File::lock_shared(&gate).unwrap();

        let payload = tokio::time::timeout(Duration::from_millis(500), cache.get(&key(), &token))
            .await
            .expect("cache hits must not serialize behind another reader");
        assert_eq!(payload.unwrap(), "payload");
    }

    #[tokio::test]
    async fn killed_downloader_releases_lock() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let mut process = child(directory.path(), "hold").spawn().unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            while !directory.path().join("locked").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        process.kill().unwrap();
        process.wait().unwrap();
        ready.unwrap();
        let guard = tokio::time::timeout(Duration::from_secs(2), cache.load_lock(&key()))
            .await
            .unwrap();
        assert!(guard.is_some());
    }

    #[tokio::test]
    async fn hit_does_not_wait_for_directory_writer() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let token = cache.generation();
        cache
            .put(&key(), Bytes::from_static(b"value"), &token)
            .await;
        let gate = open_lock(&directory.path().join("directory.lock")).unwrap();
        gate.lock().unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), cache.get(&key(), &token))
                .await
                .unwrap()
                .unwrap(),
            "value"
        );
        // A marker alone must fail closed, even before generation rotation.
        std::fs::write(directory.path().join(MUTATION_MARKER), b"").unwrap();
        assert!(cache.get(&key(), &token).await.is_none());
    }

    #[tokio::test]
    async fn opened_block_survives_invalidation_but_new_reads_reject_old_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let token = cache.generation();
        cache.put(&key(), Bytes::from_static(b"old"), &token).await;
        let encoded_key = generation_key(&key(), token.as_ref().unwrap());
        let gate = read_lock(directory.path().join("directory.lock"))
            .await
            .unwrap();
        let mut opened =
            File::open(directory.path().join(encoded_key.cache_relative_path())).unwrap();
        drop(gate);
        cache.invalidate().await.unwrap();
        assert!(cache.get(&key(), &token).await.is_none());
        let current = cache.generation();
        cache
            .put(&key(), Bytes::from_static(b"new"), &current)
            .await;
        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).unwrap();
        assert_eq!(decode_block_owned(&encoded_key, bytes).unwrap(), "old");
        assert_eq!(cache.get(&key(), &current).await.unwrap(), "new");
    }

    #[tokio::test]
    async fn hits_refresh_stale_recency_but_do_not_touch_recent_blocks() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let token = cache.generation();
        cache
            .put(&key(), Bytes::from_static(b"value"), &token)
            .await;
        let path = directory
            .path()
            .join(generation_key(&key(), token.as_ref().unwrap()).cache_relative_path());
        let recent = std::time::SystemTime::now() - Duration::from_secs(1);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(recent)
            .unwrap();
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert!(cache.get(&key(), &token).await.is_some());
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            before
        );
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(recent - Duration::from_secs(60))
            .unwrap();
        assert!(cache.get(&key(), &token).await.is_some());
        assert!(std::fs::metadata(&path).unwrap().modified().unwrap() > before);
    }

    #[tokio::test]
    async fn capacity_and_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 512).unwrap();
        assert!(SharedDiskCache::new(directory.path().to_path_buf(), 1024).is_err());
        let token = cache.generation();
        for index in 0..10 {
            let mut key = key();
            key.block_index = index;
            cache.put(&key, Bytes::from(vec![42; 64]), &token).await;
        }
        assert!(existing_usage(directory.path()) <= 512);
        let newest = BlockKey::with_namespace("catalog", "data.parquet", 64, 9);
        let file = directory
            .path()
            .join(generation_key(&newest, token.as_ref().unwrap()).cache_relative_path());
        std::fs::write(file, b"corrupt").unwrap();
        assert!(cache.get(&newest, &token).await.is_none());
    }

    #[tokio::test]
    async fn shared_cache_preserves_metadata_priority() {
        let directory = tempfile::tempdir().unwrap();
        let cache =
            SharedDiskCache::new_with_ratio(directory.path().to_path_buf(), 2048, 0.25).unwrap();
        assert!(
            SharedDiskCache::new_with_ratio(directory.path().to_path_buf(), 2048, 0.5).is_err()
        );
        let token = cache.generation();
        let metadata = BlockKey::with_namespace("catalog", "snapshot-1", 64, 0);
        cache.put(&metadata, Bytes::from(vec![1; 64]), &token).await;
        for index in 0..20 {
            let mut data = key();
            data.block_index = index;
            cache.put(&data, Bytes::from(vec![2; 64]), &token).await;
        }
        assert_eq!(
            cache.get(&metadata, &token).await,
            Some(Bytes::from(vec![1; 64]))
        );
        assert!(existing_usage(directory.path()) <= 2048);
    }

    #[tokio::test]
    async fn interrupted_accounting_recovers_before_next_publication() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 512).unwrap();
        let token = cache.generation();
        cache.put(&key(), Bytes::from(vec![42; 64]), &token).await;
        // Simulate exit after publishing a block, before committing its size.
        std::fs::write(directory.path().join("usage"), b"0").unwrap();
        std::fs::write(directory.path().join("dirty"), b"").unwrap();
        let mut second = key();
        second.block_index = 1;
        cache.put(&second, Bytes::from(vec![43; 64]), &token).await;
        assert!(!directory.path().join("dirty").exists());
        let used: u64 = std::fs::read_to_string(directory.path().join("usage"))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(used, existing_usage(directory.path()));
        assert!(used <= 512);
    }

    #[tokio::test]
    async fn download_locks_allow_different_blocks_of_the_same_file() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 1024 * 1024).unwrap();
        let first = key();
        let mut second = first.clone();
        // Select a different stripe; collisions remain intentionally bounded.
        loop {
            second.block_index += 1;
            if first.cache_relative_path().components().next()
                != second.cache_relative_path().components().next()
            {
                break;
            }
        }
        let guard = cache.load_lock(&first).await.unwrap();
        let other = tokio::time::timeout(Duration::from_secs(1), cache.load_lock(&second))
            .await
            .expect("another block must not wait for this file's download")
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), cache.load_lock(&first))
                .await
                .is_err()
        );
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), cache.load_lock(&first))
                .await
                .unwrap()
                .is_some()
        );
        drop(other);
    }

    #[tokio::test]
    async fn interrupted_temporary_blocks_are_removed_before_clean_startup() {
        for invalidate in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let cache = SharedDiskCache::new(directory.path().to_path_buf(), 1024 * 1024).unwrap();
            let token = cache.generation();
            let block = directory
                .path()
                .join(generation_key(&key(), token.as_ref().unwrap()).cache_relative_path());
            std::fs::create_dir_all(block.parent().unwrap()).unwrap();
            let temporary = block.parent().unwrap().join(format!(
                ".{}.tmp.{}",
                block.file_name().unwrap().to_string_lossy(),
                uuid::Uuid::new_v4()
            ));
            // Simulate a publisher exiting after writing a partial temporary block.
            std::fs::write(&temporary, b"partial block").unwrap();
            std::fs::write(directory.path().join("dirty"), b"").unwrap();
            if invalidate {
                cache.invalidate().await.unwrap();
            } else {
                cache
                    .put(&key(), Bytes::from_static(b"payload"), &token)
                    .await;
                assert_eq!(cache.get(&key(), &token).await.unwrap(), "payload");
            }
            assert!(!temporary.exists());
            assert!(clean_usage(directory.path(), 1024 * 1024).is_some());
            assert!(child(directory.path(), "startup")
                .status()
                .unwrap()
                .success());
            assert!(!temporary.exists());
        }
    }

    #[test]
    fn clean_workers_do_not_scan_blocks_on_startup() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 1024 * 1024).unwrap();
        let generation = cache.generation().unwrap();
        let mut used = 0;
        for index in 0..2000 {
            let key = generation_key(
                &BlockKey::with_namespace("catalog", "data.parquet", 64, index),
                &generation,
            );
            let path = directory.path().join(key.cache_relative_path());
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let bytes = encode_block(&key, &Bytes::from_static(b"payload"));
            used += bytes.len() as u64;
            std::fs::write(path, bytes).unwrap();
        }
        finish_usage(directory.path(), used).unwrap();
        let mut children: Vec<_> = (0..8)
            .map(|_| child(directory.path(), "startup").spawn().unwrap())
            .collect();
        for process in &mut children {
            assert!(process.wait().unwrap().success());
        }
    }

    #[test]
    fn startup_recovers_dirty_invalid_and_oversized_accounting() {
        let directory = tempfile::tempdir().unwrap();
        SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        for usage in ["invalid", "4097", "0"] {
            std::fs::write(directory.path().join("usage"), usage).unwrap();
            if usage == "0" {
                std::fs::write(directory.path().join("dirty"), b"").unwrap();
            }
            let before = RECOVERY_SCANS.with(|count| count.get());
            SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
            assert_eq!(RECOVERY_SCANS.with(|count| count.get()), before + 1);
            assert_eq!(clean_usage(directory.path(), 4096), Some(0));
        }
    }

    #[tokio::test]
    async fn startup_replaces_malformed_generation_and_unknown_blocks() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let old_generation = cache.generation();
        cache
            .put(&key(), Bytes::from_static(b"old"), &old_generation)
            .await;
        let old_block = directory
            .path()
            .join(generation_key(&key(), old_generation.as_ref().unwrap()).cache_relative_path());
        assert!(old_block.exists());
        std::fs::write(directory.path().join("generation"), b"truncated").unwrap();

        let repaired = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let new_generation = repaired.generation();
        assert!(new_generation.is_some());
        assert_ne!(new_generation, old_generation);
        assert!(!old_block.exists());
        assert_eq!(clean_usage(directory.path(), 4096), Some(0));

        repaired
            .put(&key(), Bytes::from_static(b"new"), &new_generation)
            .await;
        assert_eq!(repaired.get(&key(), &new_generation).await.unwrap(), "new");
    }

    #[tokio::test]
    async fn invalidation_reports_lock_timeout_and_filesystem_failure() {
        let directory = tempfile::tempdir().unwrap();
        let cache = SharedDiskCache::new(directory.path().to_path_buf(), 4096).unwrap();
        let gate = open_lock(&directory.path().join("directory.lock")).unwrap();
        gate.lock().unwrap();
        assert!(cache.invalidate().await.is_err());
        drop(gate);
        std::fs::create_dir(directory.path().join("generation.tmp")).unwrap();
        assert!(cache.invalidate().await.is_err());
        std::fs::remove_dir(directory.path().join("generation.tmp")).unwrap();
        cache.invalidate().await.unwrap();
    }
}
