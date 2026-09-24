<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# C Integration

The C integration exposes Apache Paimon Rust through a C ABI. It provides
catalog and table access, scan planning, predicate push-down, streaming reads,
writes and commits, and vector search. Record batches cross the ABI through the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

The C binding is currently built from source. The repository does not check in
a generated header or publish pre-built C packages.

## Prerequisites

- A Rust toolchain supported by this repository
- A C11-compatible compiler
- [`cbindgen`](https://github.com/mozilla/cbindgen) for generating the C header
- An Arrow implementation if the application reads or writes record batches

Install `cbindgen` when it is not already available:

```bash
cargo install cbindgen --locked
```

## Building the Library and Header

Run the following commands from the repository root:

```bash
cargo build --release -p paimon-c
cbindgen bindings/c --lang c --output target/release/paimon.h
```

The build produces a dynamic library and a static library under
`target/release/`. Dynamic library names are platform-specific:

| Platform | Dynamic library |
|----------|-----------------|
| Linux | `libpaimon_c.so` |
| macOS | `libpaimon_c.dylib` |
| Windows | `paimon_c.dll` |

Link the generated header and library into an application:

```bash
cc -std=c11 example.c \
  -Itarget/release \
  -Ltarget/release \
  -lpaimon_c \
  -o example
```

Make the dynamic library visible when running the executable. For example:

```bash
# Linux
LD_LIBRARY_PATH=target/release ./example /path/to/warehouse

# macOS
DYLD_LIBRARY_PATH=target/release ./example /path/to/warehouse
```

## Opening and Scanning a Table

The following program opens `default.my_table` from a filesystem catalog and
plans its data splits. Result structs contain either the requested handle or a
non-null error.

```c
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "paimon.h"

#define CHECK_RESULT(result)                                                \
    do {                                                                    \
        if ((result).error != NULL) {                                       \
            fprintf(stderr, "Paimon error %d: %.*s\n",                     \
                    (result).error->code,                                   \
                    (int)(result).error->message.len,                       \
                    (const char *)(result).error->message.data);            \
            paimon_error_free((result).error);                              \
            goto cleanup;                                                   \
        }                                                                   \
    } while (0)

int main(int argc, char **argv) {
    int status = EXIT_FAILURE;
    paimon_catalog *catalog = NULL;
    paimon_identifier *identifier = NULL;
    paimon_table *table = NULL;
    paimon_read_builder *read_builder = NULL;
    paimon_table_scan *scan = NULL;
    paimon_plan *plan = NULL;

    if (argc != 2) {
        fprintf(stderr, "usage: %s WAREHOUSE\n", argv[0]);
        return EXIT_FAILURE;
    }

    paimon_option options[] = {
        {.key = "warehouse", .value = argv[1]},
    };
    paimon_result_catalog_new catalog_result =
        paimon_catalog_create(options, 1);
    CHECK_RESULT(catalog_result);
    catalog = catalog_result.catalog;

    paimon_result_identifier_new identifier_result =
        paimon_identifier_new("default", "my_table");
    CHECK_RESULT(identifier_result);
    identifier = identifier_result.identifier;

    paimon_result_get_table table_result =
        paimon_catalog_get_table(catalog, identifier);
    CHECK_RESULT(table_result);
    table = table_result.table;

    paimon_result_read_builder builder_result =
        paimon_table_new_read_builder(table);
    CHECK_RESULT(builder_result);
    read_builder = builder_result.read_builder;

    paimon_result_table_scan scan_result =
        paimon_read_builder_new_scan(read_builder);
    CHECK_RESULT(scan_result);
    scan = scan_result.scan;

    paimon_result_plan plan_result = paimon_table_scan_plan(scan);
    CHECK_RESULT(plan_result);
    plan = plan_result.plan;

    printf("planned splits: %zu\n", paimon_plan_num_splits(plan));
    status = EXIT_SUCCESS;

cleanup:
    paimon_plan_free(plan);
    paimon_table_scan_free(scan);
    paimon_read_builder_free(read_builder);
    paimon_table_free(table);
    paimon_identifier_free(identifier);
    paimon_catalog_free(catalog);
    return status;
}
```

Catalog options are the same options accepted by the Rust catalog factory. For
example, a REST catalog can be created with:

```c
paimon_option options[] = {
    {.key = "metastore", .value = "rest"},
    {.key = "uri", .value = "http://localhost:8080"},
    {.key = "warehouse", .value = "my_warehouse"},
};

paimon_result_catalog_new result = paimon_catalog_create(options, 3);
```

## Caller-managed File Cache

Native engines can create a reusable `paimon_file_io` and connect Paimon reads
to their own block-cache framework. The callback table is versioned so future
extensions do not change an existing C ABI:

```c
paimon_file_cache_callbacks_v1 cache = {
    .context = engine_cache,
    .get = engine_cache_get,
    .put = engine_cache_put,
    .invalidate_path = engine_cache_invalidate_path,
    .invalidate_prefix = engine_cache_invalidate_prefix,
    .destroy = engine_cache_release,
};

paimon_result_file_io_new io_result =
    paimon_file_io_create_with_cache_v1(
        "s3://bucket/table",
        storage_options,
        storage_options_len,
        &cache,
        1024 * 1024,
        "meta,global-index");
CHECK_RESULT(io_result);

paimon_result_get_table table_result =
    paimon_table_from_schema_json_with_file_io(
        io_result.file_io,
        "s3://bucket/table",
        table_schema_json,
        "default",
        "orders",
        NULL);
CHECK_RESULT(table_result);

// The table retained a FileIO clone.
paimon_file_io_free(io_result.file_io);
```

`get` receives an output buffer with exactly `length` writable bytes. It returns
the number of bytes copied, `-1` for a miss, or any other negative value for a
fail-open error. A nonnegative value different from `length` is also treated as
a miss. `put` and invalidation failures are ignored so object storage remains
the source of truth.

Callbacks may execute concurrently on arbitrary blocking-worker threads and
must not throw or unwind across the C ABI. Callback cache keys use
`path_data + path_length` rather than null-terminated strings because canonical
storage keys may contain embedded NUL separators. Paths and buffers are borrowed
only for the duration of each call. After successful FileIO creation, Paimon
owns `context`; `destroy` runs exactly once after the FileIO handle and all
tables cloned from it have been freed.

## Reading Arrow Record Batches

Paimon uses a **scan-then-read** flow. A scan creates a plan, and a table read
consumes a range of that plan's splits through a streaming Arrow reader:

```c
paimon_result_new_read read_result =
    paimon_read_builder_new_read(read_builder);
CHECK_RESULT(read_result);
paimon_table_read *read = read_result.read;

size_t split_count = paimon_plan_num_splits(plan);
paimon_result_record_batch_reader reader_result =
    paimon_table_read_to_arrow(read, plan, 0, split_count);
CHECK_RESULT(reader_result);
paimon_record_batch_reader *reader = reader_result.reader;

for (;;) {
    paimon_result_next_batch next = paimon_record_batch_reader_next(reader);
    CHECK_RESULT(next);

    if (next.batch.array == NULL && next.batch.schema == NULL) {
        break; /* End of stream. */
    }

    /* Import next.batch.array and next.batch.schema with an Arrow C Data
       Interface consumer before freeing their container structs. */
    paimon_arrow_batch_free(next.batch);
}

paimon_record_batch_reader_free(reader);
paimon_table_read_free(read);
```

### Shared Reader Memory Budget

Create one resource context for reads that should share a reservation limit, then
attach it to each read builder before calling `paimon_read_builder_new_read`:

```c
paimon_result_resource_context budget_result =
    paimon_resource_context_create(64 * 1024 * 1024);
CHECK_RESULT(budget_result);
paimon_resource_context *budget = budget_result.context;

paimon_error *error = paimon_read_builder_with_resources(read_builder, budget);
if (error != NULL) {
    paimon_error_free(error);
    paimon_resource_context_free(budget);
    goto cleanup;
}

/* Create and consume readers from read_builder here. */

paimon_resource_metrics metrics;
error = paimon_resource_context_metrics(budget, &metrics);
if (error != NULL) {
    paimon_error_free(error);
    paimon_resource_context_free(budget);
    goto cleanup;
}
printf("current=%zu peak=%zu\n",
       metrics.reserved_memory_bytes,
       metrics.peak_reserved_memory_bytes);

paimon_resource_context_free(budget);
```

The builder clones the context, and each read stream retains its own clone.
The caller may free the context handle after attaching it to the builder; keep
the handle until after reading if metrics are needed. Reusing one context across
builders makes their reservations compete for the same limit. A zero-byte limit
rejects nonempty reservations. Admission failure is reported with
`ResourceExhausted` (error code `6`), often from
`paimon_record_batch_reader_next` when the stream actually reads data.

`reserved_memory_bytes` is the current outstanding reservation total;
`peak_reserved_memory_bytes` is the highest total reached by this context and
does not reset when streams end. After all streams using a context are freed,
the current total returns to zero. These counters track estimated reader
working memory, including projected Parquet row groups. They are not process
allocation or RSS measurements. Returned Arrow batches retained by the caller
are not charged to the reader context.

`paimon_table_read_to_arrow` accepts an `offset` and `length`, so separate
workers can process disjoint contiguous ranges of the same plan. The requested
range is clamped to the number of available splits.

!!! note "Arrow ownership"
    After importing a returned batch with the Arrow C Data Interface, call
    `paimon_arrow_batch_free` to release the heap-allocated `ArrowArray` and
    `ArrowSchema` container structs. When writing, the ownership direction is
    reversed: `paimon_table_write_write_arrow_batch` consumes the exported
    Arrow structures, so the caller must not release them again.

## Projection and Predicates

Projection uses a null-terminated array of column names:

```c
const char *columns[] = {"id", "name", NULL};
paimon_error *error =
    paimon_read_builder_with_projection(read_builder, columns);
if (error != NULL) {
    /* Inspect error->code and error->message, then free the error. */
    paimon_error_free(error);
}
```

Predicate literals are passed as a tagged `paimon_datum`. This example builds
`id = 42` for an `INT` column and transfers the predicate to the read builder:

```c
paimon_datum value = {0};
value.tag = 3;       /* INT */
value.int_val = 42;

paimon_result_predicate predicate_result =
    paimon_predicate_equal(table, "id", value);
CHECK_RESULT(predicate_result);

paimon_error *error = paimon_read_builder_with_filter(
    read_builder, predicate_result.predicate);
if (error != NULL) {
    paimon_error_free(error);
}
```

The supported datum tags are:

| Tag | Paimon type | Value fields |
|-----|-------------|--------------|
| 0 | `BOOL` | `int_val` (`0` or non-zero) |
| 1–4 | `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `int_val` |
| 5–6 | `FLOAT`, `DOUBLE` | `double_val` |
| 7 | `STRING` | `str_data`, `str_len` |
| 8–9 | `DATE`, `TIME` | `int_val` |
| 10–11 | `TIMESTAMP`, `TIMESTAMP WITH LOCAL TIME ZONE` | `int_val`, `int_val2` |
| 12 | `DECIMAL` | `int_val`, `int_val2`, `uint_val`, `uint_val2` |
| 13 | `BYTES` | `str_data`, `str_len` |

Leaf constructors support comparisons, null checks, `IN`, string operations,
and ranges. Combine predicates with `paimon_predicate_and`,
`paimon_predicate_or`, and `paimon_predicate_not`.

!!! warning "Predicate ownership"
    `paimon_read_builder_with_filter`, the compound predicate functions, and
    `paimon_vector_search_builder_with_filter` consume their predicate inputs.
    Do not reuse or free a predicate after passing it to one of these functions.
    A predicate that has not been consumed must be released with
    `paimon_predicate_free`.

## Vector Scan, Plan, and Read

DE and primary-key vector searches use the same execution API:

1. Configure a `paimon_vector_search_builder` with the column, query, limit,
   options, predicate, and output projection.
2. Call `paimon_vector_search_builder_new_scan` and
   `paimon_vector_search_builder_new_read` to create independent owned handles.
   Creating a scan only requires the column; query validation happens when
   creating the reader.
3. Call `paimon_vector_scan_plan` to resolve the source snapshot and search work.
4. Pass the reader and plan to `paimon_vector_read_read`. It returns the usual
   Arrow record-batch reader with projected columns and `__paimon_search_score`.

`paimon_vector_search_builder_execute_read` remains the convenience operation
for local planning and reading. A plan can also be reused with different query
vectors or limits. Its table, column, and pre-filter must match the reader.

For Java-planned PK bucket work, decode each standalone
`BucketVectorSearchSplit.serialize` buffer using
`paimon_bucket_vector_search_split_deserialize`, then pass the decoded handles
to `paimon_vector_scan_plan_from_bucket_splits`. The returned common vector
plan is consumed by the same `paimon_vector_read_read` API. No table snapshot
or index manifest is read during this plan construction; supplied files, row
ranges, and snapshot IDs remain authoritative. Top-K is local to the supplied
buckets, so a distributed caller merges its per-worker results.

The decoder accepts the versioned `PKVSPLIT` format. It does not accept Java
`ObjectOutputStream` envelopes or DE `IndexVectorSearchSplit` /
`RawVectorSearchSplit` object serialization. DE plans are currently obtained
through `paimon_vector_scan_plan`.

| Owned handle | Release function |
|--------------|------------------|
| `paimon_vector_scan` | `paimon_vector_scan_free` |
| `paimon_vector_read` | `paimon_vector_read_free` |
| `paimon_vector_plan` | `paimon_vector_plan_free` |
| `paimon_bucket_vector_search_split` | `paimon_bucket_vector_search_split_free` |

Decoded splits own their data, so input bytes can be released after decoding.
Plan construction copies the split metadata and leaves input handles intact,
including on failure. Free split handles after constructing the plan. Scans and
readers can outlive their builder; plans can outlive their scan. A read borrows
its plan, and the returned Arrow stream can outlive both the reader and plan.

### Java-planned PK Bucket Splits

Java plans the search once, and the caller sends each worker its assigned
`BucketVectorSearchSplit` buffers. The native worker follows this flow:

```text
Java VectorScan.Plan
  -> BucketVectorSearchSplit.serialize(DataOutputView), one buffer per split
  -> Application transport
  -> paimon_bucket_vector_search_split_deserialize, one handle per buffer
  -> paimon_vector_scan_plan_from_bucket_splits
  -> paimon_vector_read_read
  -> paimon_record_batch_reader_next
  -> Arrow consumer and global Top-K merge
```

`paimon_vector_search_builder_new_scan` creates the scan configuration; it does
not scan storage. For this path, construct the plan with
`paimon_vector_scan_plan_from_bucket_splits`. Calling `paimon_vector_scan_plan`
or `paimon_vector_search_builder_execute_read` would plan from the table again
and would not use the worker's assigned Java splits.

#### Serialize on the Java Side

Use the standalone serializer directly with `DataOutputViewStreamWrapper` over
a byte buffer. A Java object stream adds an envelope that the C decoder does
not accept. The following helper accepts a builder already configured with the
vector column and any planning predicates, for example one obtained from
`table.newVectorSearchBuilder().withVectorColumn("embedding")`:

```java
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.table.source.VectorSearchBuilder;
import org.apache.paimon.table.source.VectorSearchSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class VectorSplitSerializer {
    public static List<byte[]> planAndSerialize(VectorSearchBuilder builder)
            throws IOException {
        VectorScan.Plan plan = builder.newVectorScan().scan();
        List<byte[]> buffers = new ArrayList<>();
        for (VectorSearchSplit split : plan.splits()) {
            if (!(split instanceof BucketVectorSearchSplit)) {
                throw new IllegalArgumentException("Expected a PK bucket vector split");
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(bytes);
            ((BucketVectorSearchSplit) split).serialize(out);
            out.flush();
            buffers.add(bytes.toByteArray());
        }
        return buffers; // Assign whole buffers to workers.
    }
}
```

Preserve each buffer's length through transport. Decode one complete buffer at
a time; do not concatenate splits into a single decoder input. The buffer starts
with the eight bytes `PKVSPLIT`, followed by the big-endian format version
(currently `1`). Use matching Java and Rust format versions.

Send the table location, branch and resolved Paimon `TableSchema` JSON alongside
the assigned buffers. The schema must retain its field IDs, primary keys and
table options, including vector index type, dimension, metric and deletion-vector
settings. This is Paimon schema JSON (`TableSchema.toString()`), not Arrow schema
JSON. Supply storage credentials/options separately when constructing the native
table; they are not merged into the table schema. A worker can use
`paimon_table_from_schema_json` without opening a catalog, or
`paimon_table_from_schema_json_with_file_io` with its own cache-enabled FileIO.
It must be able to access the data, deletion and index files named by the splits.

For example, use the received metadata and worker-local storage options to
create the table (check `opened.error` before using `opened.table`):

```c
paimon_result_get_table opened = paimon_table_from_schema_json(
    table_path, table_schema_json, database, table_name, branch,
    storage_options, storage_options_len);
```

Pass `opened.table` to the helper below and release it with `paimon_table_free`
after use. Pass `NULL, 0` for storage options when none are needed, and `NULL`
for `branch` only when the Java planner used the default `main` branch.

The split buffers carry planned work, not the query vector, Top-K limit,
projection, query options or an executable scalar predicate. Send these query
parameters separately. If a scalar pre-filter is required, reconstruct it with
the `paimon_predicate_*` APIs and attach it to the native builder before creating
both scan and reader. Java file pruning and row ranges do not necessarily encode
the entire residual predicate. Applying that residual only after native Top-K
can discard winners without retrieving the next matching rows. PK data predicates
require deletion vectors enabled and merge-on-read disabled.

#### Read the Assigned Splits through C

This C11 helper searches `embedding`, projects `id`, and passes each batch to a
caller-provided callback. Adjust the column names to the table schema. The caller
provides a live table handle, a non-empty query of the configured dimension, a
positive `top_k`, and its assigned buffers as `paimon_byte_slice` values.

The helper borrows the table and buffers and consumes the optional `filter`,
including on failure. The callback returns zero on success. It must either use
the batch synchronously or import/move its Arrow contents, marking the source
structures released according to the Arrow C Data Interface. It must not free
the Paimon batch container itself; the helper does that after the callback,
including when the callback fails.

```c
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "paimon.h"

typedef int (*vector_batch_consumer)(void *context, paimon_arrow_batch batch);

#define VECTOR_TRY(expression)                     \
    do {                                          \
        error = (expression);                     \
        if (error != NULL) goto cleanup;           \
    } while (0)

int read_vector_splits(
    const paimon_table *table,
    const paimon_byte_slice *wire_splits, size_t split_count,
    const float *query, size_t dimension, size_t top_k,
    paimon_predicate *filter,
    vector_batch_consumer consume_batch, void *context) {
    int status = -1;
    paimon_error *error = NULL;
    paimon_bucket_vector_search_split **splits = NULL;
    paimon_vector_search_builder *builder = NULL;
    paimon_vector_scan *scan = NULL;
    paimon_vector_plan *plan = NULL;
    paimon_vector_read *read = NULL;
    paimon_record_batch_reader *reader = NULL;
    const char *projection[] = {"id", NULL};

    if (table == NULL || consume_batch == NULL) goto cleanup;
    if (split_count == 0) {
        status = 0; // No assigned work; the plan API requires non-empty input.
        goto cleanup;
    }
    if (wire_splits == NULL) goto cleanup;
    splits = calloc(split_count, sizeof(*splits));
    if (splits == NULL) goto cleanup;

    for (size_t i = 0; i < split_count; ++i) {
        paimon_result_bucket_vector_search_split decoded =
            paimon_bucket_vector_search_split_deserialize(
                wire_splits[i].data, wire_splits[i].len);
        splits[i] = decoded.split;
        VECTOR_TRY(decoded.error);
    }
    // All metadata is now owned by split handles; wire buffers can be released.

    paimon_result_vector_search_builder built =
        paimon_table_new_vector_search_builder(table);
    builder = built.builder;
    VECTOR_TRY(built.error);
    VECTOR_TRY(paimon_vector_search_builder_with_vector_column(builder, "embedding"));
    VECTOR_TRY(paimon_vector_search_builder_with_query_vector(builder, query, dimension));
    VECTOR_TRY(paimon_vector_search_builder_with_limit(builder, top_k));
    VECTOR_TRY(paimon_vector_search_builder_with_projection(builder, projection));
    VECTOR_TRY(paimon_vector_search_builder_with_filter(builder, filter));
    filter = NULL; // Ownership transferred to the builder.
    // Set paimon_vector_search_builder_with_options here if the query needs it.

    paimon_result_vector_scan scanned = paimon_vector_search_builder_new_scan(builder);
    scan = scanned.scan;
    VECTOR_TRY(scanned.error);
    paimon_result_vector_read reading = paimon_vector_search_builder_new_read(builder);
    read = reading.read;
    VECTOR_TRY(reading.error);

    paimon_result_vector_plan planned = paimon_vector_scan_plan_from_bucket_splits(
        scan, (const paimon_bucket_vector_search_split *const *)splits, split_count);
    plan = planned.plan;
    VECTOR_TRY(planned.error);

    // Plan construction copied the metadata and did not consume the handles.
    for (size_t i = 0; i < split_count; ++i) {
        paimon_bucket_vector_search_split_free(splits[i]);
        splits[i] = NULL;
    }
    paimon_vector_scan_free(scan);
    scan = NULL;
    paimon_vector_search_builder_free(builder);
    builder = NULL;

    paimon_result_record_batch_reader searched = paimon_vector_read_read(read, plan);
    reader = searched.reader;
    VECTOR_TRY(searched.error);
    // The returned stream owns what it needs, independently of these handles.
    paimon_vector_read_free(read);
    read = NULL;
    paimon_vector_plan_free(plan);
    plan = NULL;

    for (;;) {
        paimon_result_next_batch next = paimon_record_batch_reader_next(reader);
        VECTOR_TRY(next.error);
        if (next.batch.array == NULL && next.batch.schema == NULL) break;
        int consumed = consume_batch(context, next.batch);
        paimon_arrow_batch_free(next.batch);
        if (consumed != 0) goto cleanup;
    }
    status = 0;

cleanup:
    if (error != NULL) {
        fprintf(stderr, "Paimon error %d: ", error->code);
        fwrite(error->message.data, 1, error->message.len, stderr);
        fputc('\n', stderr);
        paimon_error_free(error);
    }
    paimon_record_batch_reader_free(reader);
    paimon_vector_read_free(read);
    paimon_vector_plan_free(plan);
    paimon_vector_scan_free(scan);
    paimon_vector_search_builder_free(builder);
    paimon_predicate_free(filter);
    if (splits != NULL) {
        for (size_t i = 0; i < split_count; ++i) {
            paimon_bucket_vector_search_split_free(splits[i]);
        }
        free(splits);
    }
    return status;
}

#undef VECTOR_TRY
```

Compile the helper as C and declare it with C linkage in the native worker.
When including the generated C header directly from C++, wrap the include
in `extern "C" { ... }` or generate a C++-compatible C header with
`cbindgen bindings/c --lang c --cpp-compat --output target/release/paimon.h`.
Replace the sample stderr reporting with the application's error handling as needed.

Each call returns up to `top_k` rows across all splits supplied to that call,
in relevance order, with the requested user columns and a `FLOAT32`
`__paimon_search_score` column. Higher scores rank first, including for L2 (the
score is `1 / (1 + squared_distance)`, not the raw distance). The caller must merge
the results from its disjoint assignments and apply the final global Top-K.
Include the primary-key columns in the projection if the coordinator needs
them for row identity. This merge retains the configured search mode's ANN/exact
semantics; it does not make ANN search exact.

All splits in a plan must come from one table, branch and snapshot, with at most
one split per `(partition, bucket)`. Mixed snapshot IDs and repeated buckets are
rejected. The caller is responsible for keeping the table/branch metadata paired
with the buffers and for avoiding duplicate assignments across workers. An empty
Java plan means no work; skip native plan construction. Supplied file-local row
ranges remain authoritative and are intersected with any native residual filter.

For repeated queries over the same assignment, retain `paimon_vector_plan` and
create another reader with the new query/limit instead of decoding again. Readers
must use the same table, branch, vector column and pre-filter as the plan.
Changing a builder after `new_read` does not change an already-created reader.

## Writing and Committing

Writing uses a **write-then-commit** flow:

1. Create one `paimon_write_builder` from the table.
2. Create a `paimon_table_write` and `paimon_table_commit` from that same
   builder.
3. Export each record batch through the Arrow C Data Interface and pass it to
   `paimon_table_write_write_arrow_batch`.
4. Call `paimon_table_write_prepare_commit` to obtain commit messages.
5. Pass the messages to `paimon_table_commit_commit`.
6. Free the messages, writer, committer, and builder.

### Shared Writer Memory Reservations

Attach an optional resource context to each write builder before creating its
writer. Use `paimon_write_builder_with_resources` for a standard builder or
`paimon_postpone_fixed_bucket_write_builder_with_resources` for a postpone
fixed-bucket builder. The same context can be attached to multiple builders,
including read builders, so their reservations share one limit. Both write
builder functions clone the context; the caller may free the original C handle
after attaching it. Each created writer retains the context it needs even if
its builder is freed.

These functions return an error for a null builder or context and leave the
builder unchanged. A zero-byte limit rejects nonempty reservations when writing
or preparing a commit. The existing `ResourceExhausted` error code is `6`.
Use `paimon_resource_context_metrics` while the C handle is available to read
the current and peak reserved bytes. The current count returns to zero after
all writers and readers holding reservations have released them; the peak count
remains.

This is a write memory reservation interface, not a limit on total process
memory. The current write path charges retained key-value input batches and
unflushed format-writer input batches. Sorting, encoding, transient routing
batches, file indexes, caller-owned Arrow input, and allocation overhead are
not fully charged. A reservation failure does not automatically flush or retry
a writer; handle the error as a failed write operation.

The input Arrow schema must match the table schema exactly, including field
count, order, names, and types. A non-nullable table field must not contain null
values.

!!! warning "Write builder consistency"
    The writer and committer must be created from the same write builder because
    they share a commit identity. Commit messages must be freed with
    `paimon_commit_messages_free` even after a successful commit. The caller
    retains message ownership and may retry a failed commit.

## Error Handling and Resource Ownership

Functions that can fail use one of two conventions:

- Constructors and terminal operations return a result struct with an `error`
  field. On success, `error` is null.
- Mutating builder functions return `paimon_error *` directly. A null pointer
  means success.

Error messages are byte buffers and are not null-terminated. Read exactly
`error->message.len` bytes, and release the entire error with
`paimon_error_free`. Do not separately call `paimon_bytes_free` on an error
message.

Every opaque handle returned by the C API has a matching free function. Free
resources in reverse construction order. Pointer-based free functions accept
null, which makes cleanup paths straightforward.
