# Implementation Plan: Bounded Async BlobDescriptor Reads

## Prerequisites

- [x] Design approved in `docs/designs/2026-07-17-blob-descriptor-bounded-async-read.md`.
- [x] Scope limited to external descriptor materialization in `DataEvolutionReader`.

## Tasks

### Task 1: Prove merged ranges overlap with a hard request bound

- **Files**: `crates/paimon/src/table/blob_resolver.rs` (modify tests first, then production helper)
- **Changes**: Add one delayed `TrackingFileRead` test which requires merged range reads to overlap while never exceeding eight in-flight calls. After observing RED, add the shared limiter and the smallest async range executor needed to turn the test GREEN.
- **Verify**: `cargo test -p paimon table::blob_resolver::tests::test_blob_range_reads_use_bounded_parallelism -- --exact` -> RED before production change, then PASS.
- **Dependencies**: None.

### Task 2: Enforce the byte admission budget and preserve ordering

- **Files**: `crates/paimon/src/table/blob_resolver.rs` (modify tests first, then production code)
- **Changes**: Add one test for a range at or above 64 MiB running exclusively and returning results by stable row mapping despite out-of-order completion. Implement weighted 1 MiB permits, capped at the full 64 MiB budget, with permits released immediately after I/O.
- **Verify**: `cargo test -p paimon table::blob_resolver::tests::test_blob_range_reads_apply_byte_budget_and_preserve_rows -- --exact` -> RED, then PASS.
- **Dependencies**: Task 1.

### Task 3: Wire concurrent range execution into descriptor columns

- **Files**: `crates/paimon/src/table/blob_resolver.rs` and `crates/paimon/src/table/data_evolution_reader.rs` (modify tests first, then production code)
- **Changes**: Add a batch-level test requiring two descriptor columns to resolve concurrently through one limiter. Pass the reader-lifetime limiter into `resolve_blob_column`, schedule prepared URI read groups concurrently, and resolve descriptor columns concurrently while tagging results with original column indices.
- **Verify**: `cargo test -p paimon table::data_evolution_reader -- blob_descriptor` -> the new regression test fails before wiring and passes after it.
- **Dependencies**: Tasks 1-2.

### Task 4: Validate cancellation, regressions, formatting, and lint

- **Files**: `crates/paimon/src/table/blob_resolver.rs`, `crates/paimon/src/table/data_evolution_reader.rs`, design and plan docs
- **Changes**: Add or extend a focused error-path test if the preceding tests do not prove permit release; refactor names and comments without adding behavior.
- **Verify**:
  - `cargo test -p paimon table::blob_resolver`
  - `cargo test -p paimon table::data_evolution_reader`
  - `cargo fmt --all -- --check`
  - `cargo clippy -p paimon --all-targets -- -D warnings`
- **Dependencies**: Tasks 1-3.

## Post-Implementation

- [x] Run the full core crate suite: `cargo test -p paimon`.
- [x] Review the final diff for unrelated changes and public API additions.
- [x] Confirm no ordinary `.blob` format behavior changed.
