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

# Batch Incremental Reading

The Rust API can plan and read changes between snapshot IDs. Snapshot ranges use
`(start_exclusive, end_inclusive]` semantics. For example, `(3, 5]` includes
snapshots 4 and 5.

## Scan Modes

| Mode | Behavior |
|------|----------|
| `IncrementalScanMode::Delta` | Reads data files added by `APPEND` snapshots in the range. |
| `IncrementalScanMode::Changelog` | Reads existing changelog files. It skips `OVERWRITE` snapshots and snapshots without changelog files. |
| `IncrementalScanMode::Auto` | Uses `Delta` when `changelog-producer=none`; otherwise uses `Changelog`. |
| `IncrementalScanMode::Diff` | Compares the complete table states at the start and end snapshots. |

`Changelog` mode does not generate missing changelog files. Configure a
`changelog-producer` when writing the table if changelog reads are required.

## Read Incremental Rows

Build the incremental plan and pass it to `TableRead::to_incremental_arrow`:

```rust
use futures::TryStreamExt;
use paimon::IncrementalScanMode;

let read_builder = table.new_read_builder();
let plan = read_builder
    .new_incremental_scan(IncrementalScanMode::Diff, 3, 5)
    .plan()
    .await?;

let reader = read_builder.new_read()?;
let batches = reader
    .to_incremental_arrow(&plan)?
    .try_collect::<Vec<_>>()
    .await?;
```

`Delta` and `Changelog` return rows from their planned files. `Diff` returns
after-image rows for inserted or updated keys and omits deleted keys. Projection
and filters configured on the read builder are applied to the output; `Diff`
still compares complete rows before applying projection.

## Read Audit-Log Rows

Use `TableRead::to_audit_log_arrow` when the output must include row kinds:

```rust
let reader = read_builder.new_read()?;
let batches = reader
    .to_audit_log_arrow(&plan)?
    .try_collect::<Vec<_>>()
    .await?;
```

The first output column is `rowkind`. `Diff` emits `+I`, `-U`, `+U`, and `-D`
records by comparing the before and after images. If table option
`table-read.sequence-number.enabled=true` is set, `_SEQUENCE_NUMBER` follows
`rowkind`.

## Diff Restrictions

`Diff` currently:

- requires a primary-key table with `merge-engine=deduplicate`;
- does not support deletion vectors or bucket rescaling between the two
  snapshots;
- supports `BOOLEAN`, integer, floating-point, character, string, and `DATE`
  columns;
- uses table option `diff.parallelism` to control concurrent
  partition-and-bucket comparisons (default: `4`, minimum: `1`).

The start snapshot must still exist because `Diff` reads both endpoint states.
An equal start and end snapshot produces an empty result.
