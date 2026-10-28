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

# Table Compaction

Rust supports small-file compaction for ordinary append tables and full-bucket
compaction for primary-key tables with fixed or dynamic buckets. `CompactBuilder` creates
a `CompactPlanner`, a `CompactExecutor`, and a `TableCommit`. Planning and
execution do not publish snapshots; `commit_compact` atomically publishes the
selected results in a `COMPACT` snapshot.

```rust
let builder = table.new_compact_builder();
let planner = builder.new_planner()?;
let executor = builder.new_executor()?;
let committer = builder.new_commit()?;

let plan = planner.plan().await?;
let mut results = Vec::new();
for task in plan.tasks() {
    results.push(executor.execute(task).await?);
}
let metrics = committer.commit_compact(results).await?;
println!("Compacted {} files into {}", metrics.files_removed, metrics.files_added);
```

Compaction uses the table's `target-file-size` option for its output size target.
Append task selection follows the batch packing rules in Java's
[`AppendCompactCoordinator`](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/append/AppendCompactCoordinator.java):

- Candidate files are strictly smaller than `target-file-size` multiplied by
  `compaction.small-file-ratio` (default 0.7). This avoids repeatedly rewriting
  output files that are slightly below the target after compression.
- Within each partition and bucket, candidates are sorted by file size.
  Each file contributes `file_size + source.split.open-file-cost` to the task
  weight. The open cost defaults to 4 MiB.
- A task is emitted once it contains at least two files and its weight reaches
  twice `target-file-size`. A remaining group is emitted only when it has at
  least `compaction.min.file-num` files (default 5, minimum 2).

There is no per-task file-count cap. An underfilled tail remains available for
planning against a later snapshot. Each plan independently captures a snapshot;
the API does not maintain a streaming coordinator's aging state.

Ordinary read splits use a separate strategy, matching Java's
[`AppendOnlySplitGenerator`](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java):
sort by minimum sequence number, then pack up to `source.split.target-size`
using `max(file_size, source.split.open-file-cost)` per file. Read split boundaries
do not determine compaction tasks, and changing `source.split.target-size` does
not change compaction grouping.

File sizes are approximate targets; normal writer rolling and
`target-file-row-num` still apply. An empty plan or result list publishes no
snapshot. Use `with_partition_filter(predicate)` to restrict planning to
partitions and `with_resources(context)` to share read/write memory budgets.

`CompactPlan`, `CompactTask` and `CompactResult` support Serde serialization.
Workers create their own executor with a table handle and local resources;
the task fixes the input snapshot, schema and output settings. Applications
can schedule independent tasks concurrently and submit completed subsets.
Results from different plans, duplicate tasks and overlapping inputs are
rejected. Executors stream input batches instead of collecting the whole task.

Concurrent appends can commit alongside compaction. A rewrite or overwrite of
an input file, a metadata change to that file, or a table schema change causes
the compaction commit to fail. Create a new plan after such a conflict.
Retrying the same plan and task subset with the same `commit_user` is
idempotent, including when result order changes. To retry after restarting
the coordinator, configure `with_commit_user(...)` with the original value
and retain the serialized results. A commit error can mean publication is
uncertain: retain submitted files rather than deleting them on error.

Append compaction preserves duplicate and null rows. Original files remain readable
through old snapshots, and append incremental scans skip `COMPACT` snapshots.
The removed-file metric counts manifest removals, not physical deletion;
snapshot expiration and storage reclamation are separate maintenance work.

## Primary-key compaction

The same API compacts primary-key tables with `deduplicate`,
`first-row`, `partial-update` or `aggregation` merge engines. Changelog producers
`none` and `input` are supported; `first-row` requires `none`.
The planner includes every active file of each selected partition/bucket,
including large files. Append size and count selection options do not split PK
buckets. A single level-0 file is also eligible. A bucket whose files already form one sorted
run at a single positive level needs no rewrite, including when the previous
compaction rolled multiple output files. Output is key ordered at the highest input level, or level 1
when all inputs are level 0.

Compaction uses each engine's existing merge function and persists its merge
state, including original sequence metadata and synthesized DELETE records:

- `deduplicate` retains the winning record, including retracts that must still
  suppress later out-of-order values.
- `first-row` retains the earliest record. Retracts are rejected unless
  `ignore-delete=true`. Its normal batch reader still skips uncompacted L0
  files; compaction makes the selected records visible above L0.
- `partial-update` combines sparse columns using the configured sequence
  groups and field aggregators. Delete payloads and group sequence values are
  preserved so subsequent sparse updates can continue from that state.
- `aggregation` writes the engine's field accumulators. Singleton records
  remain unreduced, including retracts; compound DELETE states are preserved
  rather than being discarded as they are by the query reader.

The existing engine rules for ignored deletes, unsupported retracts and
whole-record deletion also apply during compaction. If all records are
legitimately ignored, compaction can remove the selected files without
creating an output file. Key bounds, sequence bounds, delete counts and file
indexes are regenerated, using metadata statistics options for the output level.

The engine's logical merged state remains unchanged. Physical record counts and
`rows_written` can decrease as older versions are removed; the latter includes
retained retract records. Append and input-changelog incremental reads do not
emit new logical events for a compaction snapshot.

PK rewrites stream ordered physical KV batches into the same low-level file
writer used by ordinary PK writes. Sequence allocation and merge buffering stay
in the ordinary write path. Key bounds, sequence bounds, statistics and indexes
are collected incrementally without retaining all rows of an output file.
`write-buffer-size` flushes format buffers. Parquet, ORC, Avro and ROW keep the
same output file open across these flushes. Formats that retain their pending
rows after flushing (Mosaic/Vortex) close the file at the buffer threshold to
preserve bounded memory. Files also roll when the format's approximate byte
count reaches `target-file-size` or the row count reaches `target-file-row-num`.
Output chunks are materialized at row limits to avoid retaining and repeatedly
charging the entire source batch for small slices.
The merge currently supports
up to 256 overlapping sorted-run inputs, matching the query reader's limit.
There is no multipass merge yet; too many overlapping runs fail without
publishing a snapshot. The captured snapshot must remain available until each
task subset is successfully committed; retries of an already committed subset remain
idempotent after its read snapshot expires.

## Dynamic buckets

Dynamic bucket compaction (`bucket=-1`) rewrites each existing partition/bucket
without changing key assignments. HASH index files and their complete assignment
history, including deleted keys, are retained. A concurrent append may replace
the HASH index; compaction carries forward the latest manifest at commit time.
Cross-partition dynamic buckets are also supported: subsequent writers rebuild
their key-location index from the compacted data as usual. Compaction does not
rebalance buckets or reclaim HASH index capacity.

## Current scope

Compaction-generated changelogs (`full-compaction` / `lookup`) are unsupported.
Row tracking, Data Evolution, deletion vectors, query authorization, rowkind
fields, postpone buckets and dedicated blob/vector storage are unsupported.
Snapshot index manifests may contain only dynamic-bucket HASH indexes.
Embedded and sidecar file indexes are regenerated according to the captured
table options. Language bindings and
background compaction scheduling are not yet provided.
