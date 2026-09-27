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

use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct BlockKey {
    pub(super) namespace: String,
    pub(super) path: String,
    pub(super) block_size: u64,
    pub(super) block_index: u64,
}

impl BlockKey {
    #[cfg(test)]
    pub(super) fn new(path: impl Into<String>, block_size: u64, block_index: u64) -> Self {
        Self::with_namespace("", path, block_size, block_index)
    }

    pub(super) fn with_namespace(
        namespace: impl Into<String>,
        path: impl Into<String>,
        block_size: u64,
        block_index: u64,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            path: path.into(),
            block_size,
            block_index,
        }
    }

    pub(super) fn matches_path(&self, namespace: &str, path: &str) -> bool {
        self.namespace == namespace && self.path == path
    }

    pub(super) fn matches_prefix(&self, namespace: &str, prefix: &str) -> bool {
        self.namespace == namespace && path_matches_prefix(&self.path, prefix)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct LogicalPath {
    pub(super) namespace: String,
    pub(super) path: String,
}

impl LogicalPath {
    pub(super) fn new(namespace: &str, path: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            path: path.to_string(),
        }
    }

    pub(super) fn from_key(key: &BlockKey) -> Self {
        Self::new(&key.namespace, &key.path)
    }

    fn matches_prefix(&self, namespace: &str, prefix: &str) -> bool {
        self.namespace == namespace && path_matches_prefix(&self.path, prefix)
    }
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Debug)]
struct PathCacheState {
    generation: std::sync::atomic::AtomicU64,
    publish_gate: Arc<tokio::sync::RwLock<()>>,
}

#[derive(Clone)]
pub(in crate::io) struct CacheReadToken {
    generation: u64,
    state: Arc<PathCacheState>,
}

impl CacheReadToken {
    pub(super) fn is_current(&self) -> bool {
        self.state
            .generation
            .load(std::sync::atomic::Ordering::SeqCst)
            == self.generation
    }

    pub(super) async fn publish_guard(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.state.publish_gate.read().await
    }
}

#[derive(Debug)]
pub(super) struct CacheCoordinator {
    in_flight: tokio::sync::Mutex<HashMap<BlockKey, Weak<tokio::sync::Mutex<()>>>>,
    path_states: Mutex<HashMap<LogicalPath, Weak<PathCacheState>>>,
    prefix_barrier: Arc<tokio::sync::RwLock<()>>,
    file_sizes: Mutex<IndexMap<LogicalPath, u64>>,
}

impl Default for CacheCoordinator {
    fn default() -> Self {
        Self {
            in_flight: tokio::sync::Mutex::new(HashMap::new()),
            path_states: Mutex::new(HashMap::new()),
            prefix_barrier: Arc::new(tokio::sync::RwLock::new(())),
            file_sizes: Mutex::new(IndexMap::new()),
        }
    }
}

impl CacheCoordinator {
    pub(super) fn read_token(&self, namespace: &str, path: &str) -> CacheReadToken {
        let state = self.path_state(namespace, path);
        CacheReadToken {
            generation: state.generation.load(std::sync::atomic::Ordering::SeqCst),
            state,
        }
    }

    pub(super) async fn prefix_read_guard(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.prefix_barrier.read().await
    }

    fn path_state(&self, namespace: &str, path: &str) -> Arc<PathCacheState> {
        let logical_path = LogicalPath::new(namespace, path);
        let mut states = self
            .path_states
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(state) = states.get(&logical_path).and_then(Weak::upgrade) {
            return state;
        }
        if states.len() >= 1024 {
            states.retain(|_, state| state.strong_count() > 0);
        }
        let state = Arc::new(PathCacheState {
            generation: std::sync::atomic::AtomicU64::new(0),
            publish_gate: Arc::new(tokio::sync::RwLock::new(())),
        });
        states.insert(logical_path, Arc::downgrade(&state));
        state
    }

    pub(super) async fn block_load_lock(&self, key: &BlockKey) -> Arc<tokio::sync::Mutex<()>> {
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

    pub(super) async fn release_block_load_lock(
        &self,
        key: &BlockKey,
        lock: &Arc<tokio::sync::Mutex<()>>,
    ) {
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

    pub(super) fn file_size(&self, namespace: &str, path: &str) -> Option<u64> {
        let logical_path = LogicalPath::new(namespace, path);
        let mut file_sizes = self
            .file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let size = file_sizes.shift_remove(&logical_path)?;
        file_sizes.insert(logical_path, size);
        Some(size)
    }

    pub(super) fn put_file_size(&self, namespace: &str, path: &str, size: u64, capacity: usize) {
        let logical_path = LogicalPath::new(namespace, path);
        let mut file_sizes = self
            .file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        file_sizes.shift_remove(&logical_path);
        file_sizes.insert(logical_path, size);
        while file_sizes.len() > capacity {
            file_sizes.shift_remove_index(0);
        }
    }

    pub(super) async fn begin_path_invalidation(
        &self,
        namespace: &str,
        path: &str,
    ) -> CacheInvalidationGuard {
        let prefix_guard = PrefixGuard::Read {
            _guard: self.prefix_barrier.clone().read_owned().await,
        };
        let state = self.path_state(namespace, path);
        let publish_guard = state.publish_gate.clone().write_owned().await;
        state
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shift_remove(&LogicalPath::new(namespace, path));
        CacheInvalidationGuard {
            _prefix_guard: prefix_guard,
            _publish_guards: vec![publish_guard],
        }
    }

    pub(super) async fn begin_prefix_invalidation(
        &self,
        namespace: &str,
        prefix: &str,
    ) -> CacheInvalidationGuard {
        let prefix_guard = PrefixGuard::Write {
            _guard: self.prefix_barrier.clone().write_owned().await,
        };
        let prefix = prefix.trim_end_matches('/');
        let mut states = {
            let states = self
                .path_states
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            states
                .iter()
                .filter(|(path, _)| path.matches_prefix(namespace, prefix))
                .filter_map(|(path, state)| Weak::upgrade(state).map(|state| (path.clone(), state)))
                .collect::<Vec<_>>()
        };
        states.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut publish_guards = Vec::with_capacity(states.len());
        for (_, state) in states {
            publish_guards.push(state.publish_gate.clone().write_owned().await);
            state
                .generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.file_sizes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|path, _| !path.matches_prefix(namespace, prefix));
        CacheInvalidationGuard {
            _prefix_guard: prefix_guard,
            _publish_guards: publish_guards,
        }
    }
}

#[derive(Debug)]
enum PrefixGuard {
    Read {
        _guard: tokio::sync::OwnedRwLockReadGuard<()>,
    },
    Write {
        _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
    },
}

#[derive(Debug)]
pub(super) struct CacheInvalidationGuard {
    _prefix_guard: PrefixGuard,
    _publish_guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>>,
}
