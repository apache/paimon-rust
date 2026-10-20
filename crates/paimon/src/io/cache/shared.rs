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
//! A shared directory lock protects cache hits, while an exclusive lock serializes
//! publication, eviction, and invalidation. File-striped locks coalesce downloads.

use super::*;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::time::Duration;

const MUTATION_MARKER: &str = "mutation-pending";

#[derive(Debug)]
pub(crate) struct SharedDiskCache {
    root: PathBuf,
    max_size: u64,
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
    pub(crate) fn new(root: PathBuf, max_size: u64) -> crate::Result<Self> {
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
            if read_generation(&root).is_none() {
                // No block can be trusted without the epoch used to encode its
                // key. Clear unknown blocks before publishing a fresh epoch.
                std::fs::write(root.join("dirty"), b"")?;
                let used = recover_usage(&root, 0)?;
                rotate_generation(&root)?;
                finish_usage(&root, used)?;
            }
            // Clean participants share persisted accounting; do not rescan every
            // block whenever another worker starts.
            if clean_usage(&root, max_size).is_none() {
                std::fs::write(root.join("dirty"), b"")?;
                let used = recover_usage(&root, max_size)?;
                finish_usage(&root, used)?;
            }
            Ok(())
        };
        initialize().map_err(|e| crate::Error::ConfigInvalid {
            message: e.to_string(),
        })?;
        Ok(Self { root, max_size })
    }

    pub(crate) fn generation(&self) -> Option<String> {
        let generation = read_generation(&self.root)?;
        mutation_cleared(&self.root).then_some(generation)
    }

    pub(crate) async fn load_lock(&self, key: &BlockKey) -> Option<File> {
        // All blocks of one file use the same stripe, also coordinating read_full.
        let stripe =
            BlockKey::with_namespace(&key.namespace, &key.path, 0, 0).cache_relative_path();
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
        let gate = read_lock(self.root.join("directory.lock")).await?;
        let root = self.root.clone();
        let key = generation_key(key, generation.as_ref()?);
        let generation = generation.clone();
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            if !mutation_cleared(&root)
                || std::fs::read_to_string(root.join("generation")).ok() != generation
            {
                return None;
            }
            let path = root.join(key.cache_relative_path());
            let data = std::fs::read(&path).ok()?;
            let payload = decode_block(&key, &data).ok()?;
            // Persist recency so eviction observes hits from every process.
            if let Ok(file) = File::open(path) {
                let _ = file.set_modified(std::time::SystemTime::now());
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
        let Some(gate) = lock(self.root.join("directory.lock")).await else {
            return;
        };
        let root = self.root.clone();
        let key = generation_key(key, generation.as_ref().unwrap());
        let generation = generation.clone();
        let max_size = self.max_size;
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let _gate = gate;
            if !mutation_cleared(&root)
                || std::fs::read_to_string(root.join("generation")).ok() != generation
            {
                return Ok(());
            }
            let encoded = encode_block(&key, &payload);
            if encoded.len() as u64 > max_size {
                return Ok(());
            }
            let path = root.join(key.cache_relative_path());
            let mut used = if root.join("dirty").exists() {
                recover_usage(&root, max_size)?
            } else {
                match std::fs::read_to_string(root.join("usage"))
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(used) => used,
                    None => recover_usage(&root, max_size)?,
                }
            };
            // Keep the marker until publication and accounting both complete.
            // A killed process leaves it for the next writer to recover.
            std::fs::write(root.join("dirty"), b"")?;
            let mut previous = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if used
                .saturating_sub(previous)
                .saturating_add(encoded.len() as u64)
                > max_size
            {
                // Evict in batches, not one full directory scan per new block.
                used = recover_usage(
                    &root,
                    (max_size - encoded.len() as u64).min(max_size - max_size / 10),
                )?;
                previous = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            }
            let parent = path.parent().expect("cache shard");
            std::fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(
                ".{}.tmp.{}",
                path.file_name().unwrap().to_string_lossy(),
                uuid::Uuid::new_v4()
            ));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let publish = (|| {
                let mut file = options.open(&temporary)?;
                file.write_all(&encoded)?;
                drop(file);
                std::fs::rename(&temporary, &path)
            })();
            if publish.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            publish?;
            finish_usage(
                &root,
                used.saturating_sub(previous)
                    .saturating_add(encoded.len() as u64),
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
    let state = scan_existing_blocks(root);
    for key in state.entries.keys() {
        let _ = std::fs::remove_file(root.join(key.cache_relative_path()));
    }
    cleanup_temporary_files(root)?;
    finish_usage(root, scan_existing_blocks(root).current_size)
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

fn recover_usage(root: &Path, limit: u64) -> io::Result<u64> {
    // Publication holds the directory lock, so every temporary block found
    // here belongs to an interrupted publisher. Remove it before clearing dirty.
    cleanup_temporary_files(root)?;
    #[cfg(test)]
    RECOVERY_SCANS.with(|count| count.set(count.get() + 1));
    let mut state = scan_existing_blocks(root);
    state.max_size = Some(limit);
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
        let state = scan_existing_blocks(directory.path());
        assert!(state.current_size <= 512);
        let newest = BlockKey::with_namespace("catalog", "data.parquet", 64, 9);
        let file = directory
            .path()
            .join(generation_key(&newest, token.as_ref().unwrap()).cache_relative_path());
        std::fs::write(file, b"corrupt").unwrap();
        assert!(cache.get(&newest, &token).await.is_none());
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
        assert_eq!(used, scan_existing_blocks(directory.path()).current_size);
        assert!(used <= 512);
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
