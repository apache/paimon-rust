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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use arrow_array::{
    types::Int32Type, Array, ArrayRef, DictionaryArray, Int32Array, ListArray, RecordBatch,
    RecordBatchOptions, StringArray, UInt8Array,
};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::{Field, Schema};

use super::{MemoryPool, ResourceContext};
use crate::{Error, Result};

fn used(context: &ResourceContext) -> usize {
    context.metrics().reserved_memory_bytes
}

fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, array)| Field::new(i.to_string(), array.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn reservations_share_the_limit_and_failed_growth_is_atomic() {
    let resources = ResourceContext::builder().memory_limit(8).build().unwrap();
    let mut first = resources.reservation();
    let mut second = resources.clone().reservation();
    first.try_grow(8).unwrap();
    assert!(matches!(
        second.try_grow(1),
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(second.size(), 0);
    first.try_resize(3).unwrap();
    second.try_grow(5).unwrap();
    assert!(first.try_resize(9).is_err());
    assert_eq!(first.size(), 3);
    assert_eq!(used(&resources), 8);
    drop(first);
    assert_eq!(used(&resources), 5);
    drop(second);
    assert_eq!(used(&resources), 0);
    assert_eq!(resources.metrics().peak_reserved_memory_bytes, 8);
}

#[test]
fn zero_limit_and_address_space_overflow_are_rejected() {
    let zero = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut reservation = zero.reservation();
    reservation.try_grow(0).unwrap();
    assert!(reservation.try_grow(1).is_err());
    assert_eq!(used(&zero), 0);

    let unlimited = ResourceContext::builder().build().unwrap();
    let mut reservation = unlimited.reservation();
    // Reservations are accounting; this does not allocate memory.
    reservation.try_grow(usize::MAX).unwrap();
    assert!(reservation.try_grow(1).is_err());
    assert!(unlimited.reservation().try_grow(1).is_err());
    assert_eq!(used(&unlimited), usize::MAX);
    drop(reservation);
    assert_eq!(used(&unlimited), 0);
}

#[derive(Debug)]
struct EnginePool {
    limit: usize,
    reserved: AtomicUsize,
    released: AtomicUsize,
}

impl MemoryPool for EnginePool {
    fn try_reserve(&self, bytes: usize) -> Result<()> {
        self.reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map_err(|_| Error::ResourceExhausted {
                message: "engine budget".to_string(),
            })?;
        Ok(())
    }

    fn release(&self, bytes: usize) {
        assert!(self.reserved.fetch_sub(bytes, Ordering::Relaxed) >= bytes);
        self.released.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[test]
fn external_pool_failure_rolls_back_local_admission() {
    let pool = Arc::new(EnginePool {
        limit: 12,
        reserved: AtomicUsize::new(0),
        released: AtomicUsize::new(0),
    });
    let left = ResourceContext::builder()
        .memory_limit(10)
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let right = ResourceContext::builder()
        .memory_limit(10)
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let mut first = left.reservation();
    let mut second = right.reservation();
    first.try_grow(8).unwrap();
    let error = second.try_grow(5).unwrap_err();
    assert!(matches!(error, Error::ResourceExhausted { message } if message == "engine budget"));
    assert_eq!(used(&right), 0);
    assert_eq!(right.metrics().peak_reserved_memory_bytes, 0);
    second.try_grow(4).unwrap();
    assert!(first.try_grow(3).is_err());
    drop(first);
    // This reaches the local limit, proving the rejected request was rolled back.
    second.try_grow(6).unwrap();
    assert_eq!(used(&right), 10);
    assert_eq!(pool.reserved.load(Ordering::Relaxed), 10);
    drop(second);
    assert_eq!(pool.reserved.load(Ordering::Relaxed), 0);
    assert_eq!(pool.released.load(Ordering::Relaxed), 18);
}

#[test]
fn concurrent_reservations_cannot_overcommit() {
    const THREADS: usize = 16;
    let resources = ResourceContext::builder().memory_limit(32).build().unwrap();
    let start = Arc::new(Barrier::new(THREADS + 1));
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let resources = resources.clone();
            let (start, ready, release) = (start.clone(), ready.clone(), release.clone());
            std::thread::spawn(move || {
                let mut reservation = resources.reservation();
                start.wait();
                let admitted = reservation.try_grow(8).is_ok();
                ready.wait();
                release.wait();
                admitted
            })
        })
        .collect();
    start.wait();
    ready.wait();
    assert_eq!(used(&resources), 32);
    release.wait();
    let admitted = threads
        .into_iter()
        .map(|thread| usize::from(thread.join().unwrap()))
        .sum::<usize>();
    assert_eq!(admitted, 4);
    assert_eq!(used(&resources), 0);
    assert_eq!(resources.metrics().peak_reserved_memory_bytes, 32);
}

#[test]
fn output_slices_share_one_charge_and_keep_it_after_the_batch_drops() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let buffer = values.to_data().buffers()[0].clone();
    let capacity = buffer.capacity();
    let resources = ResourceContext::builder()
        .memory_limit(capacity)
        .build()
        .unwrap();
    let output = resources
        .retain_batch(batch(vec![values.clone(), values.clone()]))
        .unwrap();
    let sliced = resources.retain_batch(output.slice(1, 2)).unwrap();
    // A separately produced batch referencing the original allocation is also deduplicated.
    let sibling = resources.retain_batch(batch(vec![values])).unwrap();
    assert_eq!(used(&resources), capacity);
    assert_eq!(
        output.column(0).to_data().buffers()[0].as_ptr(),
        buffer.as_ptr()
    );
    let escaped_array = sliced.column(0).slice(1, 1);
    assert_eq!(
        escaped_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3
    );
    drop(output);
    drop(sliced);
    drop(sibling);
    assert_eq!(used(&resources), capacity);
    let escaped_buffer = escaped_array.to_data().buffers()[0].clone();
    drop(escaped_array);
    assert_eq!(used(&resources), capacity);
    drop(escaped_buffer);
    assert_eq!(used(&resources), 0);
}

#[test]
fn retaining_one_column_does_not_pin_other_columns() {
    let first: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let second: ArrayRef = Arc::new(Int32Array::from(vec![3, 4]));
    let first_capacity = first.to_data().buffers()[0].capacity();
    let second_capacity = second.to_data().buffers()[0].capacity();
    let resources = ResourceContext::builder().build().unwrap();
    let output = resources.retain_batch(batch(vec![first, second])).unwrap();
    assert_eq!(used(&resources), first_capacity + second_capacity);
    let escaped = output.column(0).clone();
    drop(output);
    assert_eq!(used(&resources), first_capacity);
    drop(escaped);
    assert_eq!(used(&resources), 0);
}

#[test]
fn nested_dictionary_and_null_buffers_preserve_values_and_lifetimes() {
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1)]),
        Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
    )
    .unwrap();
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3)]),
    ]);
    let original = batch(vec![Arc::new(dictionary), Arc::new(list)]).slice(1, 2);
    let resources = ResourceContext::builder().build().unwrap();
    let output = resources.retain_batch(original.clone()).unwrap();
    assert_eq!(output, original);
    let charged = used(&resources);
    assert!(charged > 0);
    let again = resources.retain_batch(output.clone()).unwrap();
    assert_eq!(used(&resources), charged);
    let children = again
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .clone();
    drop(output);
    drop(again);
    assert!(used(&resources) > 0);
    assert!(used(&resources) < charged);
    drop(children);
    assert_eq!(used(&resources), 0);
}

#[test]
fn external_buffer_extent_growth_and_batch_rejection_are_atomic() {
    let owner = Arc::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);
    let array = |offset, len| {
        // An external producer may expose different extents of the same backing
        // allocation in successive batches, without revealing its full capacity.
        // SAFETY: owner keeps these initialized bytes alive and immutable.
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                std::ptr::NonNull::new(owner.as_ptr().cast_mut()).unwrap(),
                offset + len,
                owner.clone(),
            )
        };
        Arc::new(UInt8Array::new(
            ScalarBuffer::new(buffer.slice_with_length(offset, len), 0, len),
            None,
        )) as ArrayRef
    };
    let resources = ResourceContext::builder().memory_limit(8).build().unwrap();
    let first = resources.retain_batch(batch(vec![array(0, 2)])).unwrap();
    assert_eq!(used(&resources), 2);
    // Extending a known external buffer and introducing a new allocation must
    // either both succeed or neither change the accounting.
    assert!(resources
        .retain_batch(batch(vec![
            array(4, 4),
            Arc::new(UInt8Array::from(vec![9; 4]))
        ]))
        .is_err());
    assert_eq!(used(&resources), 2);
    let second = resources.retain_batch(batch(vec![array(4, 4)])).unwrap();
    assert_eq!(used(&resources), 8);
    assert_eq!(
        second
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5, 6, 7, 8]
    );
    drop(second);
    assert_eq!(used(&resources), 8);
    drop(first);
    assert_eq!(used(&resources), 0);
}

#[test]
fn empty_projection_needs_no_reservation_and_preserves_row_count() {
    let resources = ResourceContext::builder().memory_limit(0).build().unwrap();
    let original = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(10)),
    )
    .unwrap();
    let output = resources.retain_batch(original).unwrap();
    assert_eq!(output.num_rows(), 10);
    assert_eq!(output.num_columns(), 0);
    assert_eq!(used(&resources), 0);
}

#[test]
fn external_views_keep_their_own_owners_alive() {
    #[derive(Debug)]
    struct Owner {
        values: Arc<Vec<u8>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    let values = Arc::new(vec![1u8, 2, 3, 4]);
    let first_drops = Arc::new(AtomicUsize::new(0));
    let second_drops = Arc::new(AtomicUsize::new(0));
    let array = |len, drops: Arc<AtomicUsize>| {
        let owner = Arc::new(Owner {
            values: values.clone(),
            drops,
        });
        // SAFETY: this owner keeps the initialized source bytes alive.
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                std::ptr::NonNull::new(owner.values.as_ptr().cast_mut()).unwrap(),
                len,
                owner,
            )
        };
        Arc::new(UInt8Array::new(ScalarBuffer::new(buffer, 0, len), None)) as ArrayRef
    };
    let resources = ResourceContext::builder().memory_limit(4).build().unwrap();
    let first = resources
        .retain_batch(batch(vec![array(2, first_drops.clone())]))
        .unwrap();
    let second = resources
        .retain_batch(batch(vec![array(4, second_drops.clone())]))
        .unwrap();
    assert_eq!(first_drops.load(Ordering::Relaxed), 0);
    assert_eq!(second_drops.load(Ordering::Relaxed), 0);
    drop(first);
    assert_eq!(first_drops.load(Ordering::Relaxed), 1);
    assert_eq!(second_drops.load(Ordering::Relaxed), 0);
    assert_eq!(used(&resources), 4);
    drop(second);
    assert_eq!(second_drops.load(Ordering::Relaxed), 1);
    assert_eq!(used(&resources), 0);
}

#[test]
fn concurrent_output_aliases_are_charged_once() {
    const THREADS: usize = 8;
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let capacity = values.to_data().buffers()[0].capacity();
    let resources = ResourceContext::builder()
        .memory_limit(capacity)
        .build()
        .unwrap();
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let (resources, values) = (resources.clone(), values.clone());
            let (ready, release) = (ready.clone(), release.clone());
            std::thread::spawn(move || {
                let output = resources.retain_batch(batch(vec![values]));
                ready.wait();
                release.wait();
                assert!(output.is_ok());
            })
        })
        .collect();
    ready.wait();
    assert_eq!(used(&resources), capacity);
    release.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(used(&resources), 0);
}

#[test]
fn output_releases_engine_reservation_after_the_context_drops() {
    let pool = Arc::new(EnginePool {
        limit: 1024,
        reserved: AtomicUsize::new(0),
        released: AtomicUsize::new(0),
    });
    let resources = ResourceContext::builder()
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let output = resources
        .retain_batch(batch(vec![Arc::new(Int32Array::from(vec![1, 2]))]))
        .unwrap();
    let bytes = pool.reserved.load(Ordering::Relaxed);
    assert!(bytes > 0);
    drop(resources);
    assert_eq!(pool.reserved.load(Ordering::Relaxed), bytes);
    std::thread::spawn(move || drop(output)).join().unwrap();
    assert_eq!(pool.reserved.load(Ordering::Relaxed), 0);
    assert_eq!(pool.released.load(Ordering::Relaxed), bytes);
}
