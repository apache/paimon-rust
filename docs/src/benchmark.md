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

# Paimon Rust at Native Speed

## TPC-DS SF100 read performance with DataFusion

!!! warning "Benchmark disclosure"

    This is a TPC-DS-derived non-TPC benchmark. It is not an official TPC
    result and must not be compared with official TPC results.

This experiment compares Paimon Rust with DataFusion's native Parquet reader
using the same DataFusion 54 SQL, planner, execution operators, query files,
runtime limits, and local SSD. It measures 24 unpartitioned, append-only
Paimon tables imported from the SF100 Parquet source.

## Result

Each query received one warmup followed by three measured iterations. The
table reports the median of three workload totals, where each workload total
is the sum of `execution_ms` for the same measured-iteration index across the
96 queries that completed in every measured iteration for both sources.

| Source | Median workload execution time | Three-run range |
| --- | ---: | ---: |
| Paimon Rust | 317.61 s | 314.64–317.76 s |
| Native Parquet | 290.53 s | 289.69–290.75 s |

Paimon Rust completes the workload within 9.3% of DataFusion's native Parquet
reader. For a table-format reader that also interprets Paimon metadata and
preserves Paimon's correctness boundaries, this is near-native performance.

The two layouts do not use the same compression codec. The source Parquet
files use SNAPPY, while the Paimon rewrite uses the higher-compression ZSTD
codec. Paimon's data files occupy 27.98 GB instead of 36.45 GB, a 23.25%
reduction in physical storage. ZSTD saves storage and read I/O at the cost of
more CPU-intensive decompression than SNAPPY, so the runtime comparison
includes that tradeoff; it does not isolate Paimon metadata overhead from
codec cost.

At the query level, Paimon has the lower median execution time on
23 of the 96 comparable queries. For the 14 queries whose native-Parquet
median is at least five seconds, the sum of per-query medians is 184.82 s for
Paimon and 179.29 s for Parquet, a gap of 3.1%. The smaller gap on the long
queries shows closer alignment as query runtime grows.

The detailed per-iteration JSON reports are not committed with this article
to keep the documentation lightweight. During validation, all 288 measured
output-row counts matched between the two sources, and the 96 normal queries
completed without warmup failures, execution errors, or spill events on
either source. The table above summarizes the validated reports using the
aggregation method described in this section.

### Resource-limit queries

Q67, Q78, and Q97 are excluded from the workload aggregate because they hit
the same 32 GiB memory-pool limit on both sources. Repeating an out-of-memory
query in the same DataFusion session can leave the benchmark runtime unable
to make progress, so these three queries were run once in separate focused
processes; the other 96 queries use the full 1+3 protocol.

## Scope

The result applies to the append-only SF100 read path tested here. The loader
creates unpartitioned append-only Paimon tables, so this experiment does
**not** measure the runtime cost of primary-key merging, schema evolution,
deletion vectors, row lineage, or non-Parquet file formats.

Paimon Rust supports or is developing those broader table-format capabilities,
and its read path includes correctness safeguards for them. They should be
evaluated with dedicated workloads before making performance claims about
their overhead.

This is also an end-to-end source comparison rather than a catalog-overhead
microbenchmark. Importing the source into Paimon rewrites the physical file
layout: the original dataset contains 24 Parquet files and the Paimon copy
contains 125 Parquet data files. It also changes compression from SNAPPY to
ZSTD, so this is intentionally not a codec-matched microbenchmark.

## Engineering behind the read path

The SF100 result follows a set of changes that align Paimon scans with
DataFusion's execution model:

- Paimon string and binary columns map to Arrow `Utf8View` and `BinaryView` at
  the DataFusion boundary.
- `read.batch-size` reaches raw readers, primary-key readers, and
  data-evolution readers.
- row counts, null counts, column bounds, and compressed sizes are exposed
  with explicit precision rather than being overstated as exact.
- Paimon predicates provide conservative partition, file, and row-group
  pruning where they can represent the expression.
- a format-neutral Arrow row-filter interface carries supported DataFusion
  expressions into the Parquet decoder for late materialization; the parent
  DataFusion filter is retained as the exact correctness filter.
- primary-key, data-evolution, and `_ROW_ID` paths use conservative boundaries
  where pushdown could change merge results or physical row positions.

Only the append-only Parquet-backed read path is benchmarked by this report;
the last point describes a correctness capability, not a measured result.

## Multi-engine direction

### DataFusion

DataFusion is the first complete SQL integration, from `SQLContext` and
`TableProvider` through statistics and physical runtime-filter pruning.

### StarRocks

Work continues on the Paimon connector, row lineage, and vector-search path.
Paimon Rust's native reader and FFI provide a foundation for deeper
integration.

### Apache Doris

The community Paimon write architecture reserves a Rust FFI backend, providing
a possible path toward removing the JVM bridge and reducing data-exchange
costs.

### Milvus

Integration work is also underway for Milvus. Paimon Rust already exposes a
materialized vector-search API through its C FFI: callers can select a vector
column, provide a query vector, filter and limit the search, pass index
options, and consume the result as streaming Arrow record batches. This gives
Milvus a native path to Paimon-managed vector data without requiring a JVM
bridge. The integration is an ecosystem direction and is not measured by this
TPC-DS report.

## Reproduction

### Revisions and software

| Component | Version or revision |
| --- | --- |
| paimon-rust benchmark binary | `7afff74a20c7dee2fe7dc1f862ce7b8b74bf6cd2` |
| Release binary SHA-256 | `1ebda8878fd2b7f4880a3fa2afaa14520e4cbc40a16352315b6a63b8aee643e7` |
| DataFusion | `54.0.0` |
| paimon-rust crate | `0.3.0` development build |
| Query files | `delta-io/delta-rs` at `0f68868d1dbbe77fa4e99c96df49ae121f8974e4` |
| Data generator | DuckDB `v1.5.0` (`3a3967aa81`) TPC-DS extension |

The benchmark binary was rebuilt from a clean Cargo target directory:

```bash
cargo clean
cargo build --release -p paimon-tpcds-bench
```

### Dataset and physical layout

| Item | Value |
| --- | ---: |
| Scale factor | 100 |
| Source Parquet files | 24 |
| Source Parquet compression | SNAPPY |
| Source Parquet physical bytes | 36,449,466,908 |
| Paimon data files | 125 |
| Paimon data-file compression | ZSTD |
| Paimon data-file physical bytes | 27,975,615,232 |
| Paimon physical data-file reduction | 8,473,851,676 bytes / 23.25% |
| All files in Paimon warehouse | 269 files / 27,975,785,742 bytes |
| Paimon table mode | unpartitioned, append-only |

The source data was generated and exported with this command:

```bash
TPCDS_ROOT=/path/to/tpcds-benchmark/sf100
DUCKDB_SPILL_ROOT=/path/to/tpcds-benchmark/spill/duckdb-sf100

./duckdb "${TPCDS_ROOT}/tpcds.duckdb" -c \
  "SET threads=12; \
   SET memory_limit='32GB'; \
   SET temp_directory='${DUCKDB_SPILL_ROOT}'; \
   CALL dsdgen(sf=100); \
   EXPORT DATABASE '${TPCDS_ROOT}/parquet' \
     (FORMAT PARQUET);"
```

The Paimon copy was loaded with one writer partition. Query execution still
used 12 target partitions.

```bash
target/release/paimon-tpcds-bench load \
  --data "${TPCDS_ROOT}/parquet" \
  --warehouse "${TPCDS_ROOT}/paimon-layout-target1-full" \
  --database tpcds \
  --if-exists error \
  --target-partitions 1 \
  --memory-limit-gib 32
```

### Machine and runtime

| Item | Value |
| --- | --- |
| Operating system | macOS 26.3.1, build 25D2128 |
| CPU | Apple M4 Pro, 12 physical cores |
| Memory | 48 GiB physical / 32 GiB DataFusion memory pool |
| Storage | Apple SSD AP1024Z, local APFS/NVMe |
| DataFusion target partitions | 12 |
| Spill directory | none for query runs |
| Cache protocol | one warmup per query, then three measured iterations |
| Source order | Alternated by query batch; see below |

The OS page cache was not evicted, so these are warm-cache measurements. To
reduce systematic source-order bias, the first source alternates by query
batch:

| Batch | Queries | Execution order |
| --- | --- | --- |
| 01 | Q1–Q10 | Paimon, Parquet |
| 02 | Q11–Q20 | Parquet, Paimon |
| 03 | Q21–Q30 | Paimon, Parquet |
| 04 | Q31–Q40 | Parquet, Paimon |
| 05 | Q41–Q50 | Paimon, Parquet |
| 06 | Q51–Q60 | Parquet, Paimon |
| 07 | Q61–Q66 | Paimon, Parquet |
| 08 | Q68–Q77 | Parquet, Paimon |
| 09 | Q79–Q88 | Paimon, Parquet |
| 10 | Q89–Q96 | Parquet, Paimon |
| 11 | Q98–Q99 | Paimon, Parquet |

### Measured commands

The following functions reproduce the arguments used for every normal batch;
set the two root paths for the local environment:

```bash
TPCDS_ROOT=/path/to/tpcds-benchmark/sf100
QUERY_ROOT=/path/to/delta-rs/crates/benchmarks/queries/tpcds

run_paimon() {
  target/release/paimon-tpcds-bench run \
    --source paimon \
    --warehouse "${TPCDS_ROOT}/paimon-layout-target1-full" \
    --database tpcds \
    --queries "${QUERY_ROOT}" \
    --output "paimon-publication-batch-$1.json" \
    --query "$2" \
    --warmup 1 \
    --iterations 3 \
    --target-partitions 12 \
    --memory-limit-gib 32
}

run_parquet() {
  target/release/paimon-tpcds-bench run \
    --source parquet \
    --data "${TPCDS_ROOT}/parquet" \
    --warehouse "${TPCDS_ROOT}/parquet-catalog-review" \
    --database tpcds \
    --queries "${QUERY_ROOT}" \
    --output "parquet-publication-batch-$1.json" \
    --query "$2" \
    --warmup 1 \
    --iterations 3 \
    --target-partitions 12 \
    --parquet-pushdown-filters \
    --memory-limit-gib 32
}

run_paimon 01 1-10;    run_parquet 01 1-10
run_parquet 02 11-20;  run_paimon 02 11-20
run_paimon 03 21-30;   run_parquet 03 21-30
run_parquet 04 31-40;  run_paimon 04 31-40
run_paimon 05 41-50;   run_parquet 05 41-50
run_parquet 06 51-60;  run_paimon 06 51-60
run_paimon 07 61-66;   run_parquet 07 61-66
run_parquet 08 68-77;  run_paimon 08 68-77
run_paimon 09 79-88;   run_parquet 09 79-88
run_parquet 10 89-96;  run_paimon 10 89-96
run_paimon 11 98-99;   run_parquet 11 98-99
```

Q67, Q78, and Q97 were each run in a fresh process with the same source
arguments and runtime settings, replacing the batch arguments with
`--query <query> --warmup 0 --iterations 1`. Both sources hit the 32 GiB
memory-pool limit for all three queries.

## Interpretation limits

This result is one machine, one data layout, and a warm-cache protocol. It is
useful as a reproducible engineering checkpoint, not a universal performance
ranking. A broader study should repeat complete benchmark processes, separate
cold and warm cache states, and include object storage and multi-node
environments.
