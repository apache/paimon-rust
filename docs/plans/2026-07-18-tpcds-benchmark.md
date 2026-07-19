# Implementation Plan: DataFusion + Paimon TPC-DS-Derived Benchmark

## Prerequisites

- [x] Confirm the benchmark contribution passes Paimon triage.
- [x] Approve the native Rust loader/runner design.
- [x] Keep the current `main` worktree and preserve unrelated user changes.

## Tasks

### Task 1: Make `SQLContext` runtime-configurable

- **Files**: `crates/integrations/datafusion/src/sql_context.rs` (modify),
  `crates/integrations/datafusion/tests/sql_context_tests.rs` (modify)
- **Changes**: Add failing tests for target-partition and runtime propagation,
  then add `new_with_config` and `new_with_config_and_runtime`. Keep `new()`
  behavior and information-schema support unchanged.
- **Verify**: `cargo test -p paimon-datafusion --test sql_context_tests custom_runtime`
  passes.
- **Dependencies**: None.

### Task 2: Add the benchmark crate and deterministic discovery helpers

- **Files**: `Cargo.toml` (modify), `benchmarks/tpcds/Cargo.toml` (create),
  `benchmarks/tpcds/src/lib.rs` (create), `benchmarks/tpcds/src/main.rs`
  (create)
- **Changes**: Add the 24 canonical table names, query/table selection parsing,
  path discovery, external query loading, and CLI validation. Start each
  behavior with a failing unit test.
- **Verify**: `cargo test -p paimon-tpcds-bench discovery` passes.
- **Dependencies**: Task 1.

### Task 3: Build benchmark runtime and catalog setup

- **Files**: `benchmarks/tpcds/src/context.rs` (create)
- **Changes**: Build `SessionConfig` and `RuntimeEnv` from target partitions,
  memory limit, spill path, and maximum spill bytes; construct a filesystem or
  in-memory Paimon catalog through the configurable `SQLContext`.
- **Verify**: focused context tests observe the requested partitions, memory
  limit, and spill directory.
- **Dependencies**: Tasks 1-2.

### Task 4: Implement Parquet-to-Paimon loading

- **Files**: `benchmarks/tpcds/src/load.rs` (create),
  `benchmarks/tpcds/tests/smoke.rs` (create)
- **Changes**: Infer one source schema, convert Arrow types to a Paimon schema,
  register a temporary Parquet view, create the target table, insert rows, and
  return a structured per-table load result. Add skip/error/overwrite policies.
- **Verify**: a tiny generated Parquet fixture loads into Paimon and returns the
  expected row count.
- **Dependencies**: Task 3.

### Task 5: Implement query execution and reporting

- **Files**: `benchmarks/tpcds/src/run.rs` (create),
  `benchmarks/tpcds/src/report.rs` (create),
  `benchmarks/tpcds/tests/smoke.rs` (modify)
- **Changes**: Register Paimon or Parquet sources, execute multi-statement query
  files, separate planning/execution timing, aggregate physical metrics, run
  configurable warmup/iterations, and serialize a versioned JSON report while
  retaining query failures.
- **Verify**: the smoke test runs the same aggregate query over Parquet and the
  loaded Paimon table, checks row counts, and round-trips the JSON report.
- **Dependencies**: Task 4.

### Task 6: Document SF10/SF100/SF1000 operation

- **Files**: `benchmarks/tpcds/README.md` (create)
- **Changes**: Document upstream data generation, load/run commands, cold/warm
  cache protocol, Parquet baseline, JSON fields, hardware guidance, and the
  required non-TPC disclosure.
- **Verify**: every documented CLI flag appears in `--help`; commands use
  external query/data paths and do not download or vendor TPC material.
- **Dependencies**: Tasks 2-5.

### Task 7: Final verification

- **Files**: all changed files
- **Changes**: Format, run focused tests, run the DataFusion integration tests,
  check the benchmark binary, run clippy for changed packages, and inspect the
  final diff for accidental scope growth.
- **Verify**: `cargo fmt --all -- --check`, `cargo test -p paimon-tpcds-bench`,
  `cargo test -p paimon-datafusion --test sql_context_tests`,
  `cargo check -p paimon-tpcds-bench`, and package-scoped clippy all pass.
- **Dependencies**: Tasks 1-6.

## Post-Implementation

- [ ] Run the tiny end-to-end smoke workflow.
- [ ] Record any queries that still need DataFusion SQL compatibility work at
  SF10 rather than silently rewriting the upstream files.
- [ ] Review the final diff before deciding whether to open a contribution PR.
