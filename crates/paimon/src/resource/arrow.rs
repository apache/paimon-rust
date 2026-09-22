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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use arrow_array::{make_array, Array, RecordBatch, RecordBatchOptions};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;

use super::{MemoryReservation, ResourceContext};
use crate::{Error, Result};

#[derive(Debug, Default)]
pub(super) struct BufferRegistry {
    allocations: Mutex<HashMap<usize, Weak<TrackedAllocation>>>,
    retired: AtomicUsize,
}

#[derive(Debug)]
struct TrackedAllocation {
    // Mutex also makes this owner RefUnwindSafe as required by Arrow Allocation,
    // without imposing that requirement on external memory-pool implementations.
    reservation: Mutex<MemoryReservation>,
    registry: Arc<BufferRegistry>,
}

impl Drop for TrackedAllocation {
    fn drop(&mut self) {
        // No registry lock here: the final owner can drop while admission holds
        // it. A later batch removes the weak entries in an amortized sweep.
        self.registry.retired.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct TrackedBuffer {
    // Retain the exact source buffer, including external owners whose backing
    // extent may differ from another view at the same base pointer. Release
    // that owner before returning this buffer's shared reservation.
    _buffer: Buffer,
    _allocation: Arc<TrackedAllocation>,
}

fn arrow_error(error: arrow_schema::ArrowError) -> Error {
    Error::DataInvalid {
        message: "Failed to attach memory reservations to Arrow buffers".to_string(),
        source: Some(Box::new(error)),
    }
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> Error {
    Error::UnexpectedError {
        message: "Arrow buffer accounting mutex was poisoned".to_string(),
        source: None,
    }
}

fn buffer_id(buffer: &Buffer) -> usize {
    buffer.data_ptr().as_ptr() as usize
}

fn visit_buffers(data: &ArrayData, visit: &mut impl FnMut(&Buffer)) {
    for buffer in data.buffers() {
        visit(buffer);
    }
    if let Some(nulls) = data.nulls() {
        visit(nulls.inner().inner());
    }
    for child in data.child_data() {
        visit_buffers(child, visit);
    }
}

impl ResourceContext {
    /// Account for retained output buffers, without copying their data.
    /// Native buffers are charged by allocation capacity. For externally owned
    /// buffers Arrow only knows the exposed extent, which can be smaller than
    /// the actual allocation; retain the largest observed charge per pointer.
    pub(crate) fn retain_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let data: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| column.to_data())
            .collect();
        let mut requested: HashMap<usize, usize> = HashMap::new();
        for array in &data {
            visit_buffers(array, &mut |buffer| {
                let bytes = buffer.capacity().max(buffer.ptr_offset() + buffer.len());
                if bytes != 0 {
                    requested
                        .entry(buffer_id(buffer))
                        .and_modify(|size| *size = (*size).max(bytes))
                        .or_insert(bytes);
                }
            });
        }

        let mut registry = self.buffers.allocations.lock().map_err(lock_error)?;
        // Sweep only after enough owners retire to pay for walking the map.
        // Scanning every live allocation for every batch would be quadratic
        // when a caller retains many batches. Weak entries retain no buffers.
        if self.buffers.retired.load(Ordering::Relaxed) >= (registry.len() / 2).max(64) {
            self.buffers.retired.store(0, Ordering::Relaxed);
            registry.retain(|_, owner| owner.strong_count() != 0);
        }
        let mut owners = HashMap::with_capacity(requested.len());
        let mut additional = 0usize;
        for (&id, bytes) in &requested {
            let owner = registry.get(&id).and_then(Weak::upgrade);
            let reserved = match &owner {
                Some(owner) => owner.reservation.lock().map_err(lock_error)?.size(),
                None => 0,
            };
            additional = additional
                .checked_add(bytes.saturating_sub(reserved))
                .ok_or_else(|| Error::ResourceExhausted {
                    message: "Arrow output buffer size exceeds addressable memory".to_string(),
                })?;
            if let Some(owner) = owner {
                owners.insert(id, owner);
            }
        }
        // Reserve the whole batch before changing any existing allocation's
        // charge. Failure therefore cannot leave a partial reservation behind.
        let mut reservation = self.reservation();
        reservation.try_grow(additional)?;
        for (id, bytes) in requested {
            if let Some(owner) = owners.get(&id) {
                let mut existing = owner.reservation.lock().map_err(lock_error)?;
                let additional = bytes.saturating_sub(existing.size());
                existing.absorb(reservation.split_off(additional));
            } else {
                let owner = Arc::new(TrackedAllocation {
                    reservation: Mutex::new(reservation.split_off(bytes)),
                    registry: Arc::clone(&self.buffers),
                });
                registry.insert(id, Arc::downgrade(&owner));
                owners.insert(id, owner);
            }
        }
        drop(registry);

        let columns = data
            .into_iter()
            .map(|data| attach_owners(data, &owners).map(make_array))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            batch.schema(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(arrow_error)
    }
}

fn attach_buffer(buffer: &Buffer, owners: &HashMap<usize, Arc<TrackedAllocation>>) -> Buffer {
    let Some(owner) = owners.get(&buffer_id(buffer)) else {
        return buffer.clone();
    };
    let offset = buffer.ptr_offset();
    // SAFETY: the original immutable buffer proves the backing pointer is valid
    // through offset + len. The owner retains that allocation for the lifetime
    // of every cloned/sliced Arrow buffer. Do not expose unused capacity, which
    // may be uninitialized. Keeping the original base pointer also lets later
    // batches find this same owner in the context's registry.
    unsafe {
        Buffer::from_custom_allocation(
            buffer.data_ptr(),
            offset + buffer.len(),
            Arc::new(TrackedBuffer {
                _buffer: buffer.clone(),
                _allocation: owner.clone(),
            }),
        )
    }
    .slice_with_length(offset, buffer.len())
}

fn attach_owners(
    data: ArrayData,
    owners: &HashMap<usize, Arc<TrackedAllocation>>,
) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| attach_buffer(buffer, owners))
        .collect();
    let nulls = data.nulls().map(|nulls| {
        let bits = nulls.inner();
        NullBuffer::new(BooleanBuffer::new(
            attach_buffer(bits.inner(), owners),
            bits.offset(),
            bits.len(),
        ))
    });
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(|child| attach_owners(child, owners))
        .collect::<Result<Vec<_>>>()?;
    data.into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()
        .map_err(arrow_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;

    #[test]
    fn expired_accounting_entries_do_not_accumulate_over_a_long_stream() {
        let resources = ResourceContext::builder()
            .memory_limit(1024)
            .build()
            .unwrap();
        // Keep source allocations alive so each batch has a distinct pointer,
        // while releasing the accounted output immediately after consumption.
        let inputs: Vec<_> = (0..1024)
            .map(|i| Arc::new(Int32Array::from(vec![i])))
            .collect();
        for input in &inputs {
            let batch =
                RecordBatch::try_from_iter([("id", input.clone() as arrow_array::ArrayRef)])
                    .unwrap();
            drop(resources.retain_batch(batch).unwrap());
        }
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        assert!(resources.buffers.allocations.lock().unwrap().len() <= 64);
    }
}
