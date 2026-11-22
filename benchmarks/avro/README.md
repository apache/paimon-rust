<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Avro data-file streaming

The data-file reader visits Avro Object Container File (OCF) blocks in
physical row order, following Java Paimon's `AvroBulkFormat`. It decompresses
one selected block at a time. A block can yield several Arrow batches, each
with at most the requested number of rows. Row selection and residual
filtering can make batches smaller. The reader includes unprojected predicate
columns when evaluating residual filters.

Row ranges use original, zero-based physical positions, with inclusive ends.
Block record counts allow whole irrelevant payloads to be skipped. The reader
still checks framing and sync markers. Reaching a selected record inside a
block requires decoding preceding records in that block. This is sequential
access, without a random-access index. Scanning stops after the final selected
row. Corruption in consumed framing, markers, payloads or records is returned
as a stream error. Unread blocks, skipped payloads and undecoded suffix records
are not validated. A header-only OCF is empty. A zero-byte file is malformed.
An empty row selection performs no I/O.

## Memory, backpressure and cancellation

Storage reads and datum decoding start on the first poll. At most one storage
request is pending. The initial read-ahead window is 512 KiB. When framing
gives the size of a selected payload, the reader fetches the remaining bytes
in requests of up to 8 MiB, including for the first block. After the first
batch, an unrestricted scan also reads ahead up to 8 MiB to reduce sequential
range GETs. A `LIMIT` that needs another block can trigger this larger
read-ahead. Explicit row ranges keep the 512 KiB read-ahead window to reduce
range GETs for near-full scans while allowing irrelevant payloads to be skipped.

Under Tokio, schema parsing, decompression, datum decoding, Arrow conversion
and residual filtering run on the blocking pool. Other executors run this
work inline. Each decode task processes at most one batch of physical
records, including rows discarded by row selection. Dropping the stream
cancels a pending read or aborts a queued decode and frees its input even
while every blocking worker is busy. A running task can finish its current
decompression and batch. Only polling the stream can start another read or
decode. Reads yield between buffer refills, including on cache hits.
The stream releases the reader and decoder before returning a terminal error
or a batch that exhausts the file or the selected ranges.

Peak memory includes the read-ahead window, the header and schema, the
compressed and decompressed block, row ranges, and one batch of Avro values
and Arrow conversion buffers. The reader releases each storage window when
it consumes the last byte. It parses the writer schema once per file and
releases the encoded block once the decoder has copied its payload and marker.
Only rows selected by position accumulate as Avro values. Arrow conversion
releases those values before residual filtering. The decoder can retain
capacity from a previous large block. A large block, header or value can
dominate memory. Consumers and storage caches can retain additional buffers.

## Comparing eager and streaming reads

The comparison uses a 40,000-row file with 98 columns, the null codec and a
64 KiB block target. Project all columns with a 1,024-row batch target.
Consume the full stream for throughput. For `LIMIT 32`, drop the stream as
soon as the consumer has 32 rows. Time from reader construction through
stream drop, recording time to first batch, consumed rows per second, storage
requests and bytes read. Run each sample in a fresh process for peak RSS.
The fixture recipe, cache policy and before/after measurements are in
[PR #1449](https://github.com/apache/paimon-rust/pull/1449). Those measurements
use earlier revisions with a 64 KiB initial read-ahead window.
