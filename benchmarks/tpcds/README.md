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

# DataFusion + Paimon TPC-DS-Derived Benchmark

This crate loads generated TPC-DS Parquet data into Apache Paimon and runs the
same external query files against Paimon or Parquet through Apache DataFusion.

> **Disclosure:** This is a TPC-DS-derived non-TPC benchmark. Its results are
> not official TPC results and must not be compared with official TPC results.

The crate does not include or download TPC tools, generated data, or query
text. Obtain those materials separately and follow their licenses.

## Prerequisites

- Build and run on the machine being measured; do not benchmark a debug build.
- Use the TPC-DS data generator and conversion instructions from
  [`apache/datafusion-benchmarks`](https://github.com/apache/datafusion-benchmarks/tree/main/tpcds).
- Keep the upstream query directory available, normally
  `datafusion-benchmarks/tpcds/queries`.
- Provide enough storage for the source data, Paimon copy, and DataFusion
  spill files. For SF1000, fast local NVMe is strongly recommended.

The expected generated-data layout is:

```text
/data/tpcds-sf1000/
  call_center.parquet/
  catalog_page.parquet/
  ...
  web_site.parquet/
```

The upstream generation flow uses scale factor 1000 for approximately 1 TB of
uncompressed generated data. Choose the generator partition count for the
target machine and retain it in the test notes. For example:

```bash
tpctools generate --benchmark tpcds \
  --scale 1000 \
  --partitions 64 \
  --generator-path /path/to/DSGen-software-code/tools \
  --output /data/tpcds

python3 tpcdsgen.py convert --scale-factor 1000 --partitions 64
```

The upstream conversion script currently contains environment-specific paths;
inspect and update them before conversion.

## Build

```bash
cargo build --release -p paimon-tpcds-bench
target/release/paimon-tpcds-bench --help
```

Keep the exact `paimon-rust` commit, generated-data scale, generator version,
file counts, physical bytes, operating system, CPU, memory, and storage model
with every published report.

## Compatibility Pass

Do not start with SF1000. Use progressively larger datasets:

1. SF10: validate table schemas and all query files.
2. SF100: validate correctness, spill configuration, and stable timings.
3. SF1000: run the final measurement without changing the validated SQL.

Use `--tables` and `--query` to isolate failures:

```bash
target/release/paimon-tpcds-bench load \
  --data /data/tpcds-sf10 \
  --warehouse /data/paimon-sf10 \
  --tables store_sales,date_dim,item

target/release/paimon-tpcds-bench run \
  --source paimon \
  --warehouse /data/paimon-sf10 \
  --queries /src/datafusion-benchmarks/tpcds/queries \
  --query 1,3-5 \
  --output results/paimon-sf10.json
```

Query selection accepts comma-separated numbers and inclusive ranges. Query
files may contain multiple SQL statements; their statements execute
sequentially and the file remains one timed benchmark unit.

## Load Paimon Tables

The loader infers each Parquet schema and creates an unpartitioned append-only
Paimon table. It processes and commits one table at a time.

```bash
target/release/paimon-tpcds-bench load \
  --data /data/tpcds-sf1000 \
  --warehouse /data/paimon-sf1000 \
  --database tpcds \
  --if-exists error \
  --target-partitions 64 \
  --memory-limit-gib 192 \
  --spill-dir /nvme/datafusion-spill \
  --max-spill-gib 1024
```

`--if-exists` is deliberately explicit:

- `error` stops before changing an existing table;
- `skip` leaves the existing table untouched;
- `overwrite` runs `INSERT OVERWRITE` from the Parquet source.

The loader's elapsed time is operational information, not part of query
performance results.

## Run the Paimon Benchmark

```bash
target/release/paimon-tpcds-bench run \
  --source paimon \
  --warehouse /data/paimon-sf1000 \
  --database tpcds \
  --queries /src/datafusion-benchmarks/tpcds/queries \
  --output results/datafusion-paimon-sf1000.json \
  --warmup 1 \
  --iterations 3 \
  --target-partitions 64 \
  --memory-limit-gib 192 \
  --spill-dir /nvme/datafusion-spill \
  --max-spill-gib 1024
```

## Run the Parquet Baseline

Use the same binary, runtime settings, query files, and generated data. The
`--warehouse` path is only a lightweight catalog location for session-scoped
Parquet tables; use an empty location separate from the measured data.

```bash
target/release/paimon-tpcds-bench run \
  --source parquet \
  --data /data/tpcds-sf1000 \
  --warehouse /data/parquet-benchmark-catalog \
  --database tpcds \
  --queries /src/datafusion-benchmarks/tpcds/queries \
  --output results/datafusion-parquet-sf1000.json \
  --warmup 1 \
  --iterations 3 \
  --target-partitions 64 \
  --memory-limit-gib 192 \
  --spill-dir /nvme/datafusion-spill \
  --max-spill-gib 1024
```

This is an end-to-end source comparison. Loading the data into Paimon rewrites
the physical files, so it is not a pure measurement of catalog or manifest
overhead.

## Cache Protocol

Run and label cold and warm experiments separately:

- Warm: use `--warmup 1` or more and report only measured iterations.
- Cold: use `--warmup 0`, start from a documented cache state, and perform OS
  page-cache eviction outside this tool only when the test operator can do so
  safely.

The runner does not drop the OS page cache. Do not mix cold and warm timings in
one aggregate.

## JSON Report

The versioned report records:

- source, paths, versions, runtime limits, and query iteration counts;
- logical planning, physical planning, execution, and total wall-clock time;
- output rows and errors;
- spill count, spilled rows, spilled bytes, bytes scanned, and summed operator
  peak-memory metrics when DataFusion exposes them.

`operator_peak_memory_bytes` is the sum of available operator metrics, not a
process-wide peak RSS. A zero value can mean that the physical operators did
not publish that metric. Paimon scans currently do not publish a precise
`bytes_scanned` execution metric, so that field may also be zero.

The report is written even when a measured query fails; the command then exits
non-zero. Preserve failed queries in comparisons instead of silently excluding
them.

## Interpreting Results

Report per-query distributions and failure counts. Useful summaries include
median, p95, total elapsed time, and geometric mean over the same successful
query set. Never hide OOM, timeout, unsupported SQL, or correctness failures by
computing an aggregate only from the remaining queries.
