# DataFusion + Paimon TPC-DS-Derived Benchmark Design

## Problem Statement

`paimon-datafusion` has functional tests for scans, writes, pruning, and SQL,
but it has no repeatable macro benchmark that exercises analytical joins,
aggregations, sorts, and spill behavior at large scale. We need a harness that
can load the Apache DataFusion benchmark Parquet data into Paimon, run the same
external TPC-DS-derived query files against both sources, and emit machine-
readable results suitable for SF10, SF100, and SF1000 runs.

The result is a non-TPC benchmark. The repository must not vendor the TPC-DS
query text or imply that the output is an official TPC result.

## Considered Approaches

### A. Native Rust loader and runner

- Add a non-published workspace binary dedicated to the benchmark.
- Infer schemas from the generated Parquet data and load append-only Paimon
  tables through the existing DataFusion sink.
- Accept the query directory from `apache/datafusion-benchmarks` at runtime.
- Run the same query files against Paimon or the original Parquet files.

This provides direct control over DataFusion's runtime, memory limit, spill
directory, physical plans, and operator metrics. It also exercises the exact
Rust integration being measured. The cost is a medium-sized benchmark crate.

### B. Extend the DataFusion Python runner

- Reuse the existing Python script and switch table registration to the
  `pypaimon-rust` binding.

This is the smallest implementation, but Python packaging and FFI become part
of the test setup, runtime configuration is less explicit, and it does not
directly validate the Rust API.

### C. Query-only Rust runner with external ingestion

- Require Spark or Flink to create the Paimon tables and provide only a Rust
  query runner.

This keeps the repository change small, but makes the data layout and ingest
procedure difficult to reproduce and prevents a self-contained smoke test.

## Chosen Approach

Approach A. It gives the most reproducible comparison while keeping dataset
generation and copyrighted query material in the upstream benchmark project.
The loader and runner are separate commands so ingest time is never mixed with
query time.

## Design Details

### Workspace and CLI

Create `benchmarks/tpcds` as a non-published workspace crate with one binary,
`paimon-tpcds-bench`.

The first version exposes two commands:

- `load`: discover `<data>/<table>.parquet` for the 24 TPC-DS tables, infer the
  Arrow schema, create append-only Paimon tables, and insert one table at a
  time.
- `run`: discover `q1.sql` through `q99.sql`, register either the Paimon tables
  or the Parquet files, run warmups and measured iterations, and write JSON.

Both commands accept explicit table/query selection for SF1/SF10 development
and failure isolation. Destructive replacement is not implicit: existing
tables are an error unless the user explicitly selects overwrite or skip.

### Data and Catalog Model

The expected input layout matches `apache/datafusion-benchmarks`:

```text
<data>/call_center.parquet/
<data>/catalog_page.parquet/
...
<data>/web_sales.parquet/
```

The loader converts the inferred Arrow fields with Paimon's existing Arrow
type conversion and creates append-only, unpartitioned tables. This is the
neutral baseline: automatic benchmark-specific partitioning would bias the
Paimon result and would no longer match the original Parquet layout.

Each source Parquet table is registered as a session-scoped temporary table.
`INSERT INTO` then uses the existing Paimon DataFusion sink, producing one
committed snapshot per loaded table.

### Runtime Configuration

`SQLContext` needs a public constructor accepting `SessionConfig` and
`RuntimeEnv`. The existing `new()` remains source-compatible and delegates to
the configurable constructor.

The benchmark records and controls:

- DataFusion target partitions;
- optional memory limit;
- optional spill directory and spill-directory size limit;
- warmup and measured iteration counts.

### Query Execution and Metrics

Query files remain external. A file may contain more than one SQL statement;
the file is the benchmark query unit and its statements execute sequentially,
matching the upstream runner.

For each measured iteration, record:

- logical and physical planning time;
- execution and total wall-clock time;
- returned row count;
- spill count, spilled rows, and spilled bytes from physical-plan metrics;
- bytes scanned and operator peak-memory metrics when the engine exposes them;
- the error string instead of losing the rest of the report.

The report also records source kind, DataFusion/Paimon versions, paths,
selected queries, runtime settings, and the non-TPC disclosure. A run exits
non-zero after writing the report if any measured query failed.

### Correctness and Verification

The benchmark does not claim query-result certification. It records output row
counts and failures, while a small repository smoke test verifies that the
same fixture can be read from Parquet, loaded into Paimon, and queried from
Paimon. Full result-set equivalence and canonical hashing are deliberately
deferred because unordered SQL results need type-aware normalization.

## Devil's Advocate Review

- Loading SF1000 through the code under test can take substantial time and
  doubles storage. Separating `load` and `run`, supporting table selection, and
  allowing a pre-existing warehouse make this operationally manageable.
- Parquet and Paimon cannot have perfectly identical physical layouts after a
  rewrite. The report therefore records the source and paths; comparisons must
  be described as end-to-end source comparisons, not a pure metadata-only
  microbenchmark.
- Warmup of all 99 queries is expensive at SF1000. Warmup is configurable and
  can be set to zero for cold-cache runs.
- Physical-plan metrics do not provide a perfect process-wide peak RSS. The
  report names the value as operator peak memory and treats missing metrics as
  unavailable rather than fabricating a value.

## Open Questions

- Whether a later benchmark profile should add an explicitly partitioned
  Paimon layout as a separate, clearly labeled scenario.
- Whether result canonicalization should become a standalone validation
  command after the SQL compatibility pass identifies ordering and type edge
  cases.

## Out of Scope

- Generating TPC-DS data or distributing TPC-owned tools and queries.
- Publishing an official TPC-DS result.
- Distributed DataFusion execution.
- Automatic OS page-cache eviction.
- Benchmark-specific table partitioning, primary keys, or indexes.
