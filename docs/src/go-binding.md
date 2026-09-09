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

# Go Integration

The Go binding uses the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
Writes copy Arrow buffers into C-owned memory because writers may retain them
after the call returns.

## Prerequisites

- Go 1.22.4 or later
- CGO enabled with a C toolchain
- Supported platforms: Linux (amd64, arm64), macOS (amd64, arm64)

## Installation

```bash
go get github.com/apache/paimon-rust/bindings/go
```

The native library is embedded and loaded automatically. Build with
`CGO_ENABLED=1`.

## Reading BlobDescriptor Values

`BlobReader` reads serialized `BlobDescriptor` values without scanning a table.
`ReadBlobs` resolves a batch in one call; `ReadBlob` handles one descriptor.

```go
package main

import (
    "database/sql"
    "log"
    "os"

    paimon "github.com/apache/paimon-rust/bindings/go"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    db, err := sql.Open("mysql", os.Getenv("STARROCKS_DSN"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    rows, err := db.Query("SELECT blob_descriptor FROM catalog.db.my_table")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    var descriptors [][]byte
    for rows.Next() {
        var descriptor []byte
        if err := rows.Scan(&descriptor); err != nil {
            log.Fatal(err)
        }
        descriptors = append(descriptors, append([]byte(nil), descriptor...))
    }
    if err := rows.Err(); err != nil {
        log.Fatal(err)
    }

    reader, err := paimon.NewBlobReader(map[string]string{
        "fs.oss.accessKeyId":     os.Getenv("OSS_ACCESS_KEY_ID"),
        "fs.oss.accessKeySecret": os.Getenv("OSS_ACCESS_KEY_SECRET"),
        "fs.oss.endpoint":        os.Getenv("OSS_ENDPOINT"),
    })
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    blobs, err := reader.ReadBlobs(descriptors)
    if err != nil {
        log.Fatal(err)
    }
    for _, blob := range blobs {
        log.Printf("read %d bytes", len(blob))
    }
}
```

The descriptor contains only URI, offset, and length. Pass OSS/S3 credentials
with the same FileIO option names used by catalogs. If StarRocks returns a hex
or base64 string, decode it to the original descriptor bytes before calling
`ReadBlobs`.

Stream a large value without holding it all in memory:

```go
stream, err := reader.OpenBlob(descriptor)
if err != nil {
    log.Fatal(err)
}
defer stream.Close()
if _, err := io.Copy(destination, stream); err != nil {
    log.Fatal(err)
}
```

For an HTTP byte range, seek relative to the descriptor and copy only that range:

```go
size, err := stream.Seek(0, io.SeekEnd)
if err != nil || start < 0 || end < start || end >= size {
    log.Fatal("invalid range")
}
if _, err := stream.Seek(start, io.SeekStart); err != nil {
    log.Fatal(err)
}
if _, err := io.CopyN(w, stream, end-start+1); err != nil {
    log.Fatal(err)
}
```

`OpenBlob` is lazy and returns an `io.ReadSeekCloser`. `ReadBlobs` groups and
merges ranges; separate streams are not merged.

For DLF temporary data tokens, reuse a table's refreshing FileIO:

```go
catalog, err := paimon.NewCatalog(map[string]string{
    "metastore":                  "rest",
    "uri":                        os.Getenv("DLF_ENDPOINT"),
    "warehouse":                  os.Getenv("DLF_CATALOG"),
    "token.provider":             "dlf",
    "dlf.region":                 os.Getenv("DLF_REGION"),
    "dlf.oss-endpoint":           os.Getenv("DLF_OSS_ENDPOINT"),
    "dlf.token-loader":           "ecs",
    "dlf.token-ecs-role-name":    os.Getenv("DLF_ECS_ROLE"),
    "data-token.enabled":         "true",
})
if err != nil {
    log.Fatal(err)
}
defer catalog.Close()
table, err := catalog.GetTable(paimon.NewIdentifier("db", "descriptor_table"))
if err != nil {
    log.Fatal(err)
}
defer table.Close()
reader, err := table.NewBlobReader()
if err != nil {
    log.Fatal(err)
}
defer reader.Close()
```

The reader and its streams keep the table FileIO and refresh DLF data tokens
before expiry. Set `dlf.oss-endpoint` when the server-provided endpoint is not
reachable from the application. Static options passed to
`paimon.NewBlobReader` are not refreshed.

Read a `MAP<STRING, BLOB>` column as descriptors, then stream one value:

Rows with null map keys cannot be represented as Arrow maps and return an error.

```go
readBuilder, err := table.NewReadBuilderWithOptions(map[string]string{
    "blob-as-descriptor": "true",
})
if err != nil {
    log.Fatal(err)
}
defer readBuilder.Close()
if err := readBuilder.WithProjection([]string{"assets"}); err != nil {
    log.Fatal(err)
}

scan, err := readBuilder.NewScan()
if err != nil {
    log.Fatal(err)
}
defer scan.Close()
plan, err := scan.Plan()
if err != nil {
    log.Fatal(err)
}
defer plan.Close()
read, err := readBuilder.NewRead()
if err != nil {
    log.Fatal(err)
}
defer read.Close()
batches, err := read.NewRecordBatchReader(plan.Splits())
if err != nil {
    log.Fatal(err)
}
defer batches.Close()

record, err := batches.NextRecord()
if err != nil {
    log.Fatal(err)
}
descriptors, err := paimon.StringBlobMapDescriptors(record.Column(0), 0)
if err != nil {
    log.Fatal(err)
}
record.Release() // the map owns its keys and descriptors
for key, descriptor := range descriptors {
	if descriptor == nil { // null BLOB
		continue
	}
    stream, err := reader.OpenBlob(descriptor)
    if err != nil {
        log.Fatal(err)
    }
    if _, err := io.Copy(destinationFor(key), stream); err != nil {
        stream.Close()
        log.Fatal(err)
    }
    stream.Close()
}
```

`StringBlobMapDescriptors` returns an ordinary Go map and remains valid after
releasing the Arrow record. To materialize small values in one merged batch:

```go
batch := make([][]byte, 0, len(descriptors))
for _, descriptor := range descriptors {
    if descriptor != nil {
        batch = append(batch, descriptor)
    }
}
values, err := reader.ReadBlobs(batch)
```

Use `OpenBlob` for large values.

Reads are grouped by URI and nearby ranges are merged. The fixed limits are a
64 KiB merge gap, 8 MiB merged span, 8 concurrent requests, and a 64 MiB
per-reader admission budget. One larger range runs alone but may exceed that
budget; use `OpenBlob` for large values. Results retain descriptor input order.

## Creating a Catalog

Use `NewCatalog` with a map of options to create a catalog. The catalog type is determined by the `metastore` option (default: `filesystem`).

```go
import paimon "github.com/apache/paimon-rust/bindings/go"

// Local filesystem
catalog, err := paimon.NewCatalog(map[string]string{
    "warehouse": "/path/to/warehouse",
})
if err != nil {
    log.Fatal(err)
}
defer catalog.Close()
```

### Alibaba Cloud OSS

```go
catalog, err := paimon.NewCatalog(map[string]string{
    "warehouse":              "oss://bucket/warehouse",
    "fs.oss.accessKeyId":     "your-access-key-id",
    "fs.oss.accessKeySecret": "your-access-key-secret",
    "fs.oss.endpoint":        "oss-cn-hangzhou.aliyuncs.com",
})
```

### REST Catalog

```go
catalog, err := paimon.NewCatalog(map[string]string{
    "metastore": "rest",
    "uri":       "http://localhost:8080",
    "warehouse": "my_warehouse",
})
```

## Writing a Table

Use `NewWriteBuilder` for ordinary and fixed-bucket tables. The `arrow.Record`
schema must match the table schema.

```go
builder, err := table.NewWriteBuilder()
if err != nil {
    log.Fatal(err)
}
defer builder.Close()

writer, err := builder.NewWrite()
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

if err := writer.WriteArrowBatch(record); err != nil {
    log.Fatal(err)
}
messages, err := writer.PrepareCommit()
if err != nil {
    log.Fatal(err)
}
defer messages.Close()

commit, err := builder.NewCommit()
if err != nil {
    log.Fatal(err)
}
defer commit.Close()

if err := commit.Commit(messages); err != nil {
    log.Fatal(err)
}
```

Call `WriteArrowBatch` multiple times before `PrepareCommit`. `WithOverwrite`
replaces the partitions touched by the batch. `Commit` is for one-shot batch
jobs: it consumes the maximum commit identifier, so later filtered retries by
the same commit user are treated as already committed. Reuse a writer across
rounds with `CommitWithIdentifier` and increasing identifiers. Multiple writers in one process
must share a commit user and merge their messages. Commit messages are
process-local and cannot be sent to another process. For primary-key
fixed-bucket tables, assign each `(partition, bucket)` to one writer before
writing; merging messages does not establish ownership.

### Postpone Fixed-Bucket Writes

For a `bucket = -2` table, the ordinary builder writes postpone files. To write
real buckets directly, use the dedicated builder with a plan mapping each
partition to its bucket count:

```go
// Plan schema: partition keys in order, then a non-null Int32 total_buckets.
schema := arrow.NewSchema([]arrow.Field{
    {Name: "dt", Type: arrow.BinaryTypes.String, Nullable: true},
    {Name: "total_buckets", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
}, nil)
rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
defer rb.Release()
rb.Field(0).(*array.StringBuilder).AppendValues([]string{"2026-08-14", "2026-08-15"}, nil)
rb.Field(1).(*array.Int32Builder).AppendValues([]int32{1, 1}, nil)
plan := rb.NewRecord()
defer plan.Release()

builder, err := table.NewPostponeFixedBucketWriteBuilderWithCommitUser("job-1")
if err != nil {
    log.Fatal(err)
}
defer builder.Close()
if err := builder.WithBucketPlan(plan); err != nil {
    log.Fatal(err)
}
writer, err := builder.NewWrite() // requires WithBucketPlan to be called first
if err != nil {
    log.Fatal(err)
}
defer writer.Close()

if err := writer.WriteArrowBatch(record); err != nil {
    log.Fatal(err)
}
messages, err := writer.PrepareCommit()
if err != nil {
    log.Fatal(err)
}
defer messages.Close()

commit, err := builder.NewCommit()
if err != nil {
    log.Fatal(err)
}
defer commit.Close()
if err := commit.Commit(messages); err != nil {
    log.Fatal(err)
}
```

An unpartitioned plan holds only `total_buckets`. Multiple writers in one process must share the plan
and commit user and assign each `(partition, bucket)` to one writer. Commit
messages are process-local. A fixed-bucket writer is single-use; create a new
writer after `PrepareCommit`.

## Reading a Table

Paimon Go uses a **scan-then-read** pattern: first scan the table to produce splits, then read data from those splits as Arrow RecordBatches.

```go
import (
    "errors"
    "fmt"
    "io"

    "github.com/apache/arrow-go/v18/arrow/array"
    paimon "github.com/apache/paimon-rust/bindings/go"
)

// Get a table from the catalog
table, err := catalog.GetTable(paimon.NewIdentifier("default", "my_table"))
if err != nil {
    log.Fatal(err)
}
defer table.Close()

// Create a read builder
rb, err := table.NewReadBuilder()
if err != nil {
    log.Fatal(err)
}
defer rb.Close()

// Step 1: Scan — produces a Plan containing DataSplits
scan, err := rb.NewScan()
if err != nil {
    log.Fatal(err)
}
defer scan.Close()

plan, err := scan.Plan()
if err != nil {
    log.Fatal(err)
}
defer plan.Close()

splits := plan.Splits()

// Step 2: Read — consumes splits and returns Arrow RecordBatches
read, err := rb.NewRead()
if err != nil {
    log.Fatal(err)
}
defer read.Close()

reader, err := read.NewRecordBatchReader(splits)
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

for {
    record, err := reader.NextRecord()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(record)
    record.Release()
}
```

## Column Projection

Use `WithProjection` to select specific columns. Only the requested columns are read, reducing I/O.

```go
rb, err := table.NewReadBuilder()
if err != nil {
    log.Fatal(err)
}
defer rb.Close()

// Only read the "id" and "name" columns
if err := rb.WithProjection([]string{"id", "name"}); err != nil {
    log.Fatal(err)
}

// Continue with scan-then-read as above...
```

## Filter Push-Down

Filter push-down prunes data at two levels:

1. **Scan planning** — skips partitions, buckets, and data files based on file-level statistics (min/max).
2. **Read-side** — applies row-level filtering via Parquet native row filters for leaf predicates.

!!! warning
    Filter push-down is a **best-effort** optimization. The returned results may still contain rows that do not satisfy the filter condition. Callers should always apply residual filtering on the returned records to ensure correctness.

### Building Predicates

Create predicates through the `PredicateBuilder` obtained from a table:

```go
pb := table.PredicateBuilder()

// Comparison predicates
pred, err := pb.Eq("id", 1)           // id = 1
pred, err := pb.NotEq("name", "bob")  // name != "bob"
pred, err := pb.Lt("id", 3)           // id < 3
pred, err := pb.Le("id", 2)           // id <= 2
pred, err := pb.Gt("id", 1)           // id > 1
pred, err := pb.Ge("id", 2)           // id >= 2

// Null checks
pred, err := pb.IsNull("name")        // name IS NULL
pred, err := pb.IsNotNull("name")     // name IS NOT NULL

// IN / NOT IN
pred, err := pb.In("id", 1, 2, 3)          // id IN (1, 2, 3)
pred, err := pb.NotIn("name", "x", "y")    // name NOT IN ("x", "y")
```

### Applying Filters

Pass a predicate to `WithFilter` on the `ReadBuilder`:

```go
rb, err := table.NewReadBuilder()
if err != nil {
    log.Fatal(err)
}
defer rb.Close()

pb := table.PredicateBuilder()
pred, err := pb.Eq("id", 1)
if err != nil {
    log.Fatal(err)
}

// Ownership of pred is transferred — do NOT close it after this call
if err := rb.WithFilter(pred); err != nil {
    log.Fatal(err)
}

// Continue with scan-then-read...
```

### Compound Predicates

Combine predicates with `And`, `Or`, and `Not`. The `predicate` sub-package provides variadic helpers:

```go
import (
    paimon "github.com/apache/paimon-rust/bindings/go"
    "github.com/apache/paimon-rust/bindings/go/predicate"
)

pb := table.PredicateBuilder()

p1, _ := pb.Ge("id", 1)
p2, _ := pb.Le("id", 3)
p3, _ := pb.Eq("name", "alice")

// id >= 1 AND id <= 3
combined, err := predicate.And(p1, p2)

// (id >= 1 AND id <= 3) OR name = "alice"
combined, err = predicate.Or(combined, p3)

// NOT (...)
negated, err := predicate.Not(combined)
```

!!! note "Predicate Ownership"
    Predicates follow a **move** ownership model. After passing a predicate to `WithFilter`, `And`, `Or`, or `Not`, the predicate is consumed and must NOT be closed or reused by the caller.

### Supported Datum Types

Predicate values are automatically converted from Go types:

| Go Type                     | Paimon Type          |
|-----------------------------|----------------------|
| `bool`                      | Bool                 |
| `int8`                      | TinyInt              |
| `int16`                     | SmallInt             |
| `int32`                     | Int                  |
| `int` / `int64`             | Int or Long          |
| `float32`                   | Float                |
| `float64`                   | Double               |
| `string`                    | String               |
| `paimon.Date`               | Date (epoch days)    |
| `paimon.Time`               | Time (millis)        |
| `paimon.Timestamp`          | Timestamp            |
| `paimon.LocalZonedTimestamp` | LocalZonedTimestamp |
| `paimon.Decimal`            | Decimal              |
| `paimon.Bytes`              | Binary               |

For special types, use the dedicated constructors:

```go
// Date as epoch days since 1970-01-01
pred, _ := pb.Eq("dt", paimon.Date(19000))

// Decimal(123.45) as DECIMAL(10,2)
pred, _ := pb.Eq("amount", paimon.NewDecimal(12345, 10, 2))

// Timestamp
pred, _ := pb.Eq("ts", paimon.Timestamp{Millis: 1700000000000, Nanos: 0})
```

## Resource Management

Paimon objects with a `Close` method hold native resources and must be closed.
Use `defer` immediately after creation:

```go
catalog, err := paimon.NewCatalog(opts)
if err != nil { log.Fatal(err) }
defer catalog.Close()

table, err := catalog.GetTable(id)
if err != nil { log.Fatal(err) }
defer table.Close()

// ... and so on for ReadBuilder, TableScan, Plan, TableRead, RecordBatchReader
```

All `Close()` methods are safe to call multiple times.

## Complete Example

```go
package main

import (
    "errors"
    "fmt"
    "io"
    "log"

    "github.com/apache/arrow-go/v18/arrow/array"
    paimon "github.com/apache/paimon-rust/bindings/go"
)

func main() {
    // 1. Open catalog and table
    catalog, err := paimon.NewCatalog(map[string]string{
        "warehouse": "/tmp/paimon-warehouse",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer catalog.Close()

    table, err := catalog.GetTable(paimon.NewIdentifier("default", "my_table"))
    if err != nil {
        log.Fatal(err)
    }
    defer table.Close()

    // 2. Configure read: projection + filter
    rb, err := table.NewReadBuilder()
    if err != nil {
        log.Fatal(err)
    }
    defer rb.Close()

    if err := rb.WithProjection([]string{"id", "name"}); err != nil {
        log.Fatal(err)
    }

    pb := table.PredicateBuilder()
    pred, err := pb.Gt("id", 0)
    if err != nil {
        log.Fatal(err)
    }
    if err := rb.WithFilter(pred); err != nil {
        log.Fatal(err)
    }

    // 3. Scan
    scan, err := rb.NewScan()
    if err != nil {
        log.Fatal(err)
    }
    defer scan.Close()

    plan, err := scan.Plan()
    if err != nil {
        log.Fatal(err)
    }
    defer plan.Close()

    // 4. Read
    read, err := rb.NewRead()
    if err != nil {
        log.Fatal(err)
    }
    defer read.Close()

    reader, err := read.NewRecordBatchReader(plan.Splits())
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    for {
        record, err := reader.NextRecord()
        if errors.Is(err, io.EOF) {
            break
        }
        if err != nil {
            log.Fatal(err)
        }

        idCol := record.Column(0).(*array.Int32)
        nameCol := record.Column(1).(*array.String)
        for i := 0; i < int(record.NumRows()); i++ {
            fmt.Printf("id=%d name=%s\n", idCol.Value(i), nameCol.Value(i))
        }
        record.Release()
    }
}
```
