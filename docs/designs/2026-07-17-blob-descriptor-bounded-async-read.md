<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Bounded Async BlobDescriptor Reads

## Problem Statement

`DataEvolutionReader` resolves external `BlobDescriptor` values serially. Descriptor
columns are awaited one at a time, and each column awaits every URI and merged range
before starting the next one. On high-latency object storage, batches containing many
non-coalescible ranges accumulate request round-trip latency.

The ordinary `.blob` format reader already overlaps reads with bounded async polling,
so this change is limited to external descriptor materialization.

## Chosen Approach

Keep the existing per-column URI grouping and range coalescing, but execute descriptor
columns and their merged range reads concurrently. All work performed by one
`DataEvolutionReader` shares a limiter with:

- at most 8 in-flight I/O operations;
- a 64 MiB admission budget, accounted in 1 MiB units;
- a range of 64 MiB or larger consuming the entire byte budget and therefore running
  without another range read in flight.

The implementation uses normal async futures and does not create Tokio tasks or OS
threads.

## Design Details

### Limiter Lifetime

An `Arc`-backed limiter is created with `DataEvolutionReader` and reused while its
stream processes splits and batches. Concurrent descriptor columns share the same
request and byte semaphores, avoiding per-column concurrency multiplication.

### Range Execution

Each column continues to:

1. deserialize descriptors;
2. group requests by URI;
3. resolve unknown lengths once per URI within that column;
4. merge nearby ranges with the existing 64 KiB gap and 8 MiB merged-span limits;
5. read merged ranges and restore values by row index.

Descriptor columns and prepared URI read groups are polled concurrently. Unknown
length metadata is resolved once per URI and shares the same request semaphore as
range reads. URI preparation within one column remains sequential; parallelizing
those uncommon metadata requests further is deferred to keep the first change
focused. A range read acquires the request permit and then the weighted byte permit
in a consistent order. Both permits cover only the actual I/O future and are
released before the result is collected. This avoids deadlock when completed results
wait to be assembled.

A single range larger than 64 MiB acquires the entire byte budget. The budget limits
concurrent admission, not the size of the required output value; such a range may
still allocate more than 64 MiB.

### Ordering and Errors

Async completion order is not observable. Column results are tagged with their
original column index, and merged range results restore cells using the existing row
indices. The rebuilt `RecordBatch` preserves schema, column, row, null, and inline
value semantics.

No detached tasks are used. Returning the first error drops remaining futures and
releases semaphore permits through RAII. Read errors retain URI and range context.

### FileRead Concurrency

`FileRead` is already `Send + Sync`, and the Row and Blob format readers already issue
overlapping position-based reads against one instance. Descriptor reads follow the
same established contract and add a tracking-reader test for overlapping calls.

## Open Questions

- Whether production workloads benefit from a configurable concurrency value.
- Whether descriptors in different columns frequently reference adjacent ranges in
  the same URI, making cross-column coalescing worthwhile.
- Whether unknown-length descriptors occur frequently enough to parallelize metadata
  lookup during URI preparation.
- Whether the byte admission budget should eventually be shared across multiple
  `DataEvolutionReader` instances in one table scan.

These require benchmarks or production traces and are not part of this change.

## Out of Scope

- Changing ordinary `.blob` format reads.
- Cross-column range coalescing.
- Prefetching future record batches.
- Adding table options or adaptive concurrency.
- Strictly budgeting the final Arrow output or completed results.
- Adding an OS thread pool.
