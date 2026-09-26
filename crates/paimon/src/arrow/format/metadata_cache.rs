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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use lru::LruCache;
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

struct Entry<V> {
    value: OnceCell<Arc<V>>,
    base_weight: usize,
    weight: AtomicUsize,
}

struct State<K, V> {
    entries: LruCache<K, Arc<Entry<V>>>,
    weight: usize,
}

pub(super) struct FileMetadataCache<K, V> {
    max_bytes: usize,
    max_entries: usize,
    state: Mutex<State<K, V>>,
}

impl<K, V> FileMetadataCache<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(super) fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            max_bytes,
            max_entries,
            state: Mutex::new(State {
                entries: LruCache::unbounded(),
                weight: 0,
            }),
        }
    }

    pub(super) fn entry_weight(key_heap_bytes: usize, value_weight: usize) -> usize {
        std::mem::size_of::<K>()
            .saturating_add(std::mem::size_of::<Entry<V>>())
            .saturating_add(key_heap_bytes)
            .saturating_add(value_weight)
    }

    pub(super) async fn get_or_try_insert_with<E, F, Fut, W>(
        &self,
        key: Option<K>,
        key_heap_bytes: usize,
        load: F,
        value_weight: W,
    ) -> Result<Arc<V>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Arc<V>, E>>,
        W: FnOnce(&V) -> usize,
    {
        let Some(key) = key else {
            return load().await;
        };
        let base_weight = Self::entry_weight(key_heap_bytes, 0);
        if self.max_bytes == 0 || base_weight > self.max_bytes {
            return load().await;
        }

        let entry = {
            let mut state = self.state.lock().unwrap();
            if let Some(entry) = state.entries.get(&key) {
                Arc::clone(entry)
            } else {
                let entry = Arc::new(Entry {
                    value: OnceCell::new(),
                    base_weight,
                    weight: AtomicUsize::new(base_weight),
                });
                state.weight = state.weight.saturating_add(base_weight);
                state.entries.put(key.clone(), Arc::clone(&entry));
                entry
            }
        };

        let value = match entry.value.get_or_try_init(load).await {
            Ok(value) => Arc::clone(value),
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                if state
                    .entries
                    .peek(&key)
                    .is_some_and(|cached| Arc::ptr_eq(cached, &entry))
                {
                    state.entries.pop(&key);
                    state.weight = state.weight.saturating_sub(entry.base_weight);
                    self.evict(&mut state);
                }
                return Err(error);
            }
        };
        let loaded_weight = entry
            .base_weight
            .saturating_add(value_weight(value.as_ref()).max(1));
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .peek(&key)
            .is_some_and(|cached| Arc::ptr_eq(cached, &entry))
            && entry.weight.load(Ordering::Relaxed) == entry.base_weight
        {
            entry.weight.store(loaded_weight, Ordering::Relaxed);
            state.weight = state
                .weight
                .saturating_sub(entry.base_weight)
                .saturating_add(loaded_weight);
            state.entries.promote(&key);
            self.evict(&mut state);
        }
        Ok(value)
    }

    fn evict(&self, state: &mut State<K, V>) {
        while state.weight > self.max_bytes || state.entries.len() > self.max_entries {
            let Some(key) = state
                .entries
                .iter()
                .rev()
                .find(|(_, entry)| entry.weight.load(Ordering::Relaxed) != entry.base_weight)
                .map(|(key, _)| key.clone())
            else {
                // In-flight loads remain addressable so callers still coalesce.
                break;
            };
            let Some(entry) = state.entries.pop(&key) else {
                state.weight = 0;
                break;
            };
            state.weight = state
                .weight
                .saturating_sub(entry.weight.load(Ordering::Relaxed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[tokio::test]
    async fn evicts_least_recently_used_entry_by_count() {
        let cache = FileMetadataCache::<String, usize>::new(usize::MAX, 2);
        let loads = AtomicUsize::new(0);

        for key in ["first", "second", "third", "first"] {
            cache
                .get_or_try_insert_with(
                    Some(key.to_string()),
                    key.len(),
                    || async {
                        loads.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, Infallible>(Arc::new(1))
                    },
                    |_| 1,
                )
                .await
                .unwrap();
        }

        assert_eq!(loads.load(Ordering::Relaxed), 4);
    }
}
