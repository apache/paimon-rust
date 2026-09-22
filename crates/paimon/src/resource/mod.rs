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

//! Shared memory reservations for native readers and future writers.
//!
//! A reservation is accounting, not an allocator. Parquet readers reserve each
//! selected row group's projected uncompressed-column estimate before data I/O,
//! including columns needed by decoder predicates. Live working estimates and
//! retained output Arrow buffers share one limit; scheduling slots and prefetch
//! windows remain independent. A working reservation is released after its
//! decoder drops, while output reservations follow the buffers' shared owners.
//!
//! Working estimates are conservative admission charges, not measurements of
//! decoder allocations. Metadata, merge state and allocation overhead are not
//! fully covered, and actual memory can exceed the estimates. Consequently this
//! is neither an RSS limit nor a bound on every allocation made while reading.

mod arrow;
mod memory;

pub use memory::{MemoryPool, MemoryReservation, ResourceMetrics};

use std::sync::Arc;

use self::arrow::BufferRegistry;
use self::memory::MemoryAccount;
use crate::Result;

/// Resources shared by readers created for one logical operation.
///
/// Clones share the limit, metrics and Arrow buffer accounting. A buffer held
/// by multiple output batches in this context is charged once; its reservation
/// outlives the reader when the caller retains the buffer or a slice of it.
/// Native buffers are charged by allocation capacity; external buffers are
/// estimated using the largest extent Arrow exposes for their backing pointer.
/// Arrays and schema metadata, as well as allocations made by the caller's
/// subsequent Arrow operations, are not charged.
///
/// Buffers are wrapped with a shared owner without copying their contents.
/// Arrow may consequently report a smaller capacity for output buffers and
/// cannot convert them back into mutable buffers without copying. Use this
/// context's metrics for its reservations, rather than summing Arrow capacities.
/// Cloning the context shares accounting; distinct contexts charge independently.
///
/// ```
/// use paimon::resource::ResourceContext;
///
/// let resources = ResourceContext::builder()
///     .memory_limit(256 * 1024 * 1024)
///     .build()?;
/// // Pass resources.clone() to each ReadBuilder::with_resources.
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
    buffers: Arc<BufferRegistry>,
}

impl ResourceContext {
    pub fn builder() -> ResourceContextBuilder {
        ResourceContextBuilder::default()
    }

    /// Create an initially empty reservation. Grow it before retaining memory.
    pub fn reservation(&self) -> MemoryReservation {
        MemoryReservation::new(Arc::clone(&self.memory))
    }

    /// Read accounting counters. The two counters are sampled independently.
    pub fn metrics(&self) -> ResourceMetrics {
        self.memory.metrics()
    }
}

/// Configure a shared accounting limit and an optional embedding-engine pool.
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
            buffers: Arc::new(BufferRegistry::default()),
        })
    }
}

#[cfg(test)]
mod tests;
