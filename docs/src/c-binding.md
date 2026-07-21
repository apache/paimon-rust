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
