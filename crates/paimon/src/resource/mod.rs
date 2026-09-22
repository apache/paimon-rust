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

//! Shared memory reservations for readers and embedding engines.
//!
//! Parquet readers reserve each selected row group's projected uncompressed
//! column size before data I/O, including columns needed by decoder predicates.
//! The decoder owns that reservation and releases it when dropped. Concurrency
//! slots and the prefetch window remain separate scheduling controls.
//!
//! Consumers are responsible for reserving memory they retain. Output batches
//! are ordinary Arrow batches: a downstream consumer that holds them must reserve
//! its own memory. The reader does not wrap their buffers or charge their aliases.
//!
//! Reservations are accounting, not an allocator or an RSS limit. Row-group
//! charges are estimates; metadata, merge state, transient batches and allocation
//! overhead are not fully covered, and actual memory can exceed the estimates.

mod memory;
pub use memory::{MemoryPool, MemoryReservation, ResourceMetrics};

use std::sync::Arc;

use self::memory::MemoryAccount;
use crate::Result;

/// Memory budget shared by consumers in one logical operation.
///
/// Clones share admission and metrics. Each consumer owns its reservations and
/// releases them when its working state is dropped. Retained output batches are
/// the caller's responsibility, including when they outlive the reader.
///
/// ```
/// use paimon::resource::ResourceContext;
///
/// let resources = ResourceContext::builder()
///     .memory_limit(256 * 1024 * 1024)
///     .build()?;
/// // Pass resources.clone() to ReadBuilder::with_resources.
/// // Other consumers reserve from the same budget for their own retained state.
/// let mut reservation = resources.reservation();
/// reservation.try_grow(1024)?;
/// assert_eq!(resources.metrics().reserved_memory_bytes, 1024);
/// drop(reservation);
/// assert_eq!(resources.metrics().reserved_memory_bytes, 0);
/// # Ok::<(), paimon::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct ResourceContext {
    memory: Arc<MemoryAccount>,
}

impl ResourceContext {
    pub fn builder() -> ResourceContextBuilder {
        ResourceContextBuilder::default()
    }

    /// Create an initially empty reservation owned by a consumer.
    pub fn reservation(&self) -> MemoryReservation {
        MemoryReservation::new(Arc::clone(&self.memory))
    }

    /// Read accounting counters. The two counters are sampled independently.
    pub fn metrics(&self) -> ResourceMetrics {
        self.memory.metrics()
    }
}

/// Configure a local limit and an optional embedding-engine pool.
#[derive(Default, Debug)]
pub struct ResourceContextBuilder {
    memory_limit: Option<usize>,
    memory_pool: Option<Arc<dyn MemoryPool>>,
}

impl ResourceContextBuilder {
    /// Limit outstanding reservations. Zero allows only zero-byte reservations.
    /// Omitting this setting leaves the context without a local limit.
    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = Some(bytes);
        self
    }

    /// Also reserve from an external pool. Both limits must permit each request.
    pub fn memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    pub fn build(self) -> Result<ResourceContext> {
        Ok(ResourceContext {
            memory: Arc::new(MemoryAccount::new(self.memory_limit, self.memory_pool)),
        })
    }
}

#[cfg(test)]
mod tests;
