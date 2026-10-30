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

Rust supports small-file compaction for ordinary append tables, normal-file
compaction for Data Evolution tables, and sorted-run compaction for primary-key
tables with fixed, dynamic or postpone buckets. For ordinary bucket compaction,
`CompactBuilder` creates
a `CompactPlanner`, a `CompactExecutor`, and a `TableCommit`. Planning and
execution do not publish snapshots; `commit_compact` atomically publishes the
selected results in a `COMPACT` snapshot.

The builder selects an append, primary-key or Data Evolution planner for the table
mode. Each `CompactTask` rewrites one partition/bucket. Postpone tables use the
separate partition-wide API described below.

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

Ordinary append and PK compaction use `target-file-size` as the output size target.
For unaware append tables (`bucket=-1`), task selection follows the batch packing rules in Java's
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

Unordered bucketed append tables (`bucket>0`, `bucket-append-ordered=false`)
use Java's `BucketedAppendCompactManager` best-effort selection instead:
files are visited in minimum-sequence order, and a window is selected once it
reaches `compaction.min.file-num`. Before that count is reached, a window whose
physical bytes reach twice `target-file-size` drops its first file and keeps
sliding. The unaware-table small-file filter and open cost do not apply.
Ordered bucketed append compaction is unsupported: it requires sequence counters
that the ordinary Rust append writer does not yet maintain.

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
PK commits also reject overlapping key ranges within a positive level, including
ranges introduced by a concurrent compaction. Overlapping L0 appends are allowed.
If the latest snapshot is below the plan's captured snapshot after a rollback,
commit rejects the uncommitted plan; create a new plan.
Retrying the same plan and task subset with the same `commit_user` is
idempotent, including when result order changes. Recovery searches only snapshots
created after the plan's captured snapshot. To retry after restarting
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
`none`, `input` and `lookup` are supported; `first-row` supports `none` or `lookup`.
For fixed or dynamic buckets, `deduplicate` with `none`, `input` or `lookup`, and
non-DV `first-row` with `none` or `lookup`, the planner chooses incremental sorted runs using Java's
universal-compaction size and count policy. Each L0 file is a
run; all files at one positive level form a single run. Normal size and run-count
triggers take precedence. If they select no work, explicit compaction forces
progress on every L0 run and absorbs adjacent runs according to their sizes. A
large higher-level run can remain in place while smaller runs are rewritten.
Without L0, positive-level maintenance requires the configured run-count and
size triggers. A single positive run needs no rewrite, even when it spans
several files.

The policy uses `num-sorted-run.compaction-trigger` (default `5`), `num-levels`
(default trigger plus one), `compaction.size-ratio` (default `1` percent), and
`compaction.max-size-amplification-percent` (default `200`). When selected runs
reach the oldest run, output goes to the highest configured or existing level;
otherwise it goes immediately below the next unselected run. Explicit full
compaction is not exposed. Without deletion vectors, `deduplicate` outputs retain winning retracts, so older values
in untouched files do not reappear after a delete or out-of-order update.

For `first-row`, both producers require actual catalog/FileIO local-cache.
The executor looks up keys in retained higher-level files. Existing accepted keys
stay in those files and produce neither a new data row nor a changelog event.
Selected positive-level rows still participate in the output. The `lookup`
producer emits only newly accepted keys, preserving their first add's row kind
and sequence. If every input key already exists, compaction can retire its
L0 inputs without creating a data or changelog file.

With deletion vectors, `deduplicate` supports the same incremental selection for
`none`, `input` and `lookup`. All three require actual catalog/FileIO local-cache.
Lookup Files preserve unfiltered physical row positions; captured deletion vectors
filter selected inputs and history before merging. A history hit moves its winning
state into the output and marks the original position deleted, even when an
out-of-order input loses. Winning retracts are omitted from data after changelog
generation, so a DELETE may publish a changelog without a new data file.

Commit atomically unions historical row deletions with the latest DV indexes and
retires rewritten inputs' ranges, preserving unrelated ranges and HASH indexes.
It rejects changed input masks, removed lookup targets and concurrently deleted
hit positions; unrelated new DV bits can be merged. Uncommitted serialized DV results must
retain their physical deletion metadata, including an empty list when appropriate.
Each output file is limited to 4,294,967,296 physical rows. Migrating files written
without DV is unsupported, including enabling DV through temporary table options.

Non-DV `partial-update` and `aggregation` with `lookup` use the same incremental
policy and require actual catalog/FileIO local-cache. The executor inserts the
nearest retained positive-level state into each changed key's merge, in sequence
order. The output contains the materialized state; lookup scans use only the
nearest positive-level state, so retained older levels are not aggregated again.

With `none` or `input`, these engines use incremental selection only when every
value field can be merged again independently of row order: `min`, `max`,
`bool_and`, `bool_or`, or integer `sum` without checked overflow. Aggregation must
have `aggregation.remove-record-on-delete=false` and no global sequence fields.
Partial update must use `ignore-delete=true`, no global sequence fields, and
protect every value field with a sequence group and one of these aggregate
functions. Each group's ordering fields retain its winning sequence and must
not also be protected fields. These modes merge only selected inputs, leaving
retained history to ordinary query merging, and do not require local-cache.

Other `none`/`input` configurations retain their previous selection: every active
file in the selected bucket participates, and output uses the highest input level
or level 1 for all-L0 inputs. This preserves sparse overwrite and order-sensitive
state such as `first_value`, finite floating-point summation, checked-overflow
behavior and reset-on-delete semantics. Existing retract/state restrictions still
apply. Append size and count options do not control PK selection.

Tasks capture their selected runs and output level. Executors validate the task
against the captured snapshot. Committers independently recompute the selection
from that snapshot and validate the files being removed and the output levels.
Older serialized incremental tasks without an output level must be replanned
before execution. Prepared results can still be published if their file changes
match the captured selection; already committed results remain recoverable
through the existing idempotent retry path.

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
- `aggregation` writes the engine's field accumulators. With `none` or `input`,
  singleton records remain unreduced, including retracts; compound DELETE states
  are preserved rather than being discarded as they are by the query reader.

The existing engine rules for ignored deletes, unsupported retracts and
whole-record deletion also apply during compaction. If all records are
legitimately ignored, compaction can remove the selected files without
creating an output file. Key bounds, sequence bounds, delete counts and file
indexes are regenerated, using metadata statistics options for the output level.
Compound `partial-update` DELETE states with field aggregators are rejected.
For both reducer engines, compound groups containing retracts are also rejected
when they use `first_value`, `first_non_null_value`, `collect`, `listagg`,
`nested_update` or `nested_partial_update`. These states may carry initialization
flags or raw DELETE payloads that cannot be reconstructed from one output row,
even when that row is an INSERT. Execution fails without publishing or deleting
the source files.
Partial-update retracts ignored by `ignore-delete=true` do not trigger this
restriction.

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

## Lookup changelog producer

Set `changelog-producer=lookup` and run the same compaction API to publish data
and changelog files atomically in a `COMPACT` snapshot. Ordinary writes produce
L0 data files; changelogs become available when those files are compacted.
Batch incremental `Changelog` / `Auto` scans and streaming follow-up scans read
the resulting changelog manifest. `Delta` scans continue to skip compaction.

For each key, the lowest positive level is its latest materialized history.
Older positive levels already contributed to that state and are not reduced
again. Compaction merges this history with the captured L0 records and emits
changes only for keys with L0 input:

- No live history and a live result: `INSERT`.
- Live history and a deleted result: `DELETE` carrying the history's values.
- Live history and a live result: `UPDATE_BEFORE` followed by `UPDATE_AFTER`.
- No live history and no live result: no changelog.

Before images retain the historical sequence metadata; after images carry the
merged result's metadata. `first-row` emits the first accepted add for previously
unseen keys, preserving its `INSERT` or `UPDATE_AFTER` kind. It considers all
input levels when choosing the earliest record. `partial-update` and `aggregation`
always run the reducer in lookup mode, including singleton retracts, as Java does.
For example, lookup compaction turns a lone `DELETE(value=5)` with `sum` into
live state `-5` when `aggregation.remove-record-on-delete=false`; an ordinary
query still omits that singleton retract before compaction. Existing state
preservation restrictions still apply. History selection precedes merging.

Java-compatible options have these defaults:

| Option | Default | Behavior |
| --- | --- | --- |
| `changelog-producer.row-deduplicate` | `false` | When true, skip update pairs whose user values are equal. |
| `changelog-producer.row-deduplicate-ignore-fields` | unset | Comma-separated fields excluded from that comparison. |
| `changelog-producer.ignore-update-before` | `false` | Omit `UPDATE_BEFORE` events. |
| `changelog-producer.ignore-delete` | `false` | Omit `DELETE` events. |

With default options, even an unchanged value produces an update pair when its
key has L0 input. Ignoring changelog events does not change compacted table data.
`changelog-file.format`, `changelog-file.compression`, `changelog-file.prefix`
and changelog statistics options apply independently of data-file settings.
Changelog files roll at the ordinary size and row limits. `CompactResult` exposes
`changelog_files()`; `CompactMetrics` reports their file, row and byte counts in
`changelog_files_added`, `changelog_rows_written` and `changelog_bytes_written`.
The existing data metrics continue to count data files and rows only.

For `deduplicate`, unselected higher-level history is read through point lookups
in disposable local Lookup Files. Each file stores ordered keys and complete KV
rows, including row kinds and sequence metadata. The first access builds it by
streaming the source through the normal schema-evolution decoder. Later keys and
executors reuse it; a missing or corrupt cache block causes a bounded rebuild
instead of being treated as a missing key. Only selected runs are rewritten.
Lookup selects candidate files by key range, retaining range pruning across
schema changes that preserve the primary-key field IDs, order and types. The
reader retains one bounded parsed SST index and one decoded IPC batch.
Failed or cancelled builds clean up their unpublished cache blocks.

The executor streams both outputs and aborts both writers on execution failure.
Prepared results include both file lists for serialization, subset commits and
idempotent retries. Commit validation checks that selected inputs remain valid
and that files in each positive level have disjoint key ranges, matching Java's
LSM conflict checks. Concurrent L0 appends and changes to unselected files are
allowed when these checks pass. New L0 files remain eligible for a later
compaction and produce no premature events.

The executor requires an actual catalog/FileIO cache (`local-cache.enabled=true`);
setting the flag in table properties alone does not attach a cache.
Deduplicate Lookup Files use that cache's memory, disk or shared-disk backend,
namespace, eviction policy and `local-cache.max-size` budget. Set `local-cache.dir` to keep
them on disk. They do not require `data` in `local-cache.whitelist`.
`lookup.cache-max-disk-size` is rejected; use `local-cache.max-size` instead.

For deduplicate lookup caching, `lookup.cache-max-memory-size` defaults to
`256 MiB` per executor and bounds working components such as input batches,
IPC payloads and SST indexes. Oversized inputs or indexes,
or a cache unable to retain the generated file, fail execution without publishing
a snapshot. Reduce `read.batch-size` for oversized input batches or increase the
appropriate budget. Cache storage is charged separately to `local-cache.max-size`.
`lookup.cache-spill-compression` accepts `none`, `zstd` (default), `lz4` or `lzo`.

## Dynamic buckets

Dynamic bucket compaction (`bucket=-1`) rewrites each existing partition/bucket
without changing key assignments. HASH index files and their complete assignment
history, including deleted keys, are retained. A concurrent append may replace
the HASH index; compaction carries forward the latest manifest at commit time.
Cross-partition dynamic buckets are also supported: subsequent writers rebuild
their key-location index from the compacted data as usual. Compaction does not
rebalance buckets or reclaim HASH index capacity.

## Postpone buckets

`PostponeCompactBuilder` publishes pending data in primary-key tables with `bucket=-2`.
The planner captures both unsorted `bucket-postpone` files and real-bucket files,
including Level-0 files produced by the fixed-bucket postpone writer. A partition
with pending or Level-0 files becomes one task. The task rewrites that partition's
complete captured data into real buckets at a positive level; a partition with
only positive-level files needs no work. Partition filters and committing subsets
of partition tasks are supported.

```rust
let builder = table.new_postpone_compact_builder();
let plan = builder.new_planner()?.plan().await?;
let executor = builder.new_executor()?;
let mut results = Vec::new();
for task in plan.tasks() {
    results.push(executor.execute(task).await?);
}
let metrics = builder.new_commit()?.commit_postpone_compact(results).await?;
```

`PostponeCompactPlan`, `PostponeCompactTask` and `PostponeCompactResult` are
separate serializable types. A task has no single bucket: `inputs()` keeps each
file with its source bucket and bucket count in captured encounter order.
`total_buckets()` is the target partition bucket count. A result's `buckets()`
groups data and changelog outputs by real target bucket. The ordinary
`new_compact_builder` and `commit_compact` reject postpone tables; the postpone
builder and committer reject ordinary tables. Both APIs share metrics, physical
PK merging and atomic snapshot publication.

Existing per-partition bucket counts take precedence. For a new partition,
`postpone.default-bucket-num` supplies an exact count. Otherwise the count is
the ceiling of pending row count divided by `postpone.target-row-num-per-bucket`,
or pending physical file bytes divided by `postpone.target-size-per-bucket`
(default 1 GiB). The minimum count is one. Compaction does not rescale existing
partitions or apply the batch writer's power-of-two rounding and parallelism cap.

Pending files are replayed serially within each writer by creation time. Equal
creation times retain captured manifest encounter order, matching Java;
original sequence numbers and file names do not break these ties.
Different writers have no arrival ordering guarantee. Replay allocates sequences
after the captured real-bucket history and retains row kinds and configured user
sequence fields. Routed batches accumulate in a shared `write-buffer-size`
budget across buckets, with retained memory charged to `ResourceContext` when
configured. Reaching that budget sorts and spills temporary Parquet KV runs;
decoder batch boundaries do not determine spill boundaries. Additional passes
merge at most 128 temporary runs at a time while preserving every physical row,
so small read batches do not exhaust the final merge's input limit. The ordinary
PK compactor then applies the merge engine and generates lookup changelogs when
enabled. Temporary runs are removed on completion or execution error. The
existing limit of 256 overlapping final inputs, including real-bucket history,
still applies; exceeding it fails execution without publishing a snapshot.

Publication atomically removes the captured inputs and adds all real-bucket
outputs in a `COMPACT` snapshot. Concurrent new postpone files remain pending
for the next plan. A concurrent real-bucket write in a rewritten partition,
overwritten input or changed input metadata makes the commit fail; plan again
from the latest snapshot. Serialized tasks retain source bucket metadata and
results retain output bucket groups for distributed execution, subset commits
and idempotent retries.
Postpone compaction requires partition keys to be included in the primary key.
ROW pending layout is selected from the writing schema and key-stat metadata,
including Java full and thin physical layouts; decoding success is not used to
guess the schema. Thin pending ROW can be rewritten after changing the persisted
output schema to non-thin mode. ROW outputs (data or lookup changelog) and real-bucket ROW inputs with
`data-file.thin-mode=true` are explicitly rejected, because the shared Rust KV
writer/reader do not support that physical layout for materialized files.

With deletion vectors, postpone compaction supports `deduplicate` with `none`,
`input` or `lookup` changelog producers and requires actual catalog/FileIO
local-cache, as other PK DV compaction does. Captured masks filter pending
physical rows before routing and filter real-bucket L0 and historical files
before merging. Winning retracts are omitted from data after lookup changelog
generation. A task can publish no data files when all keys are deleted.

Commit atomically retires the rewritten files' DV ranges, preserving masks on
unselected partitions and concurrently appended pending files, including ranges
in shared containers. Changes to a selected input's captured mask reject the
commit; unchanged masks repacked into a different container are permitted.
Serialized results must retain their `deletion_vector_updates` metadata, even
when the list is empty, for first publication. Retries of an already committed
subset remain idempotent. Enabling DV only through temporary table options does
not migrate historical files written without DV. Other merge engines and global
index manifests remain unsupported for postpone DV compaction.

## Data Evolution compaction

The same API compacts normal structured files in unaware append tables with
`data-evolution.enabled=true` and `row-tracking.enabled=true`, following Java's
`DataEvolutionCompactCoordinator` and `DataEvolutionNormalCompactTask`.

Within each partition, planning keeps every version of an aligned row-ID group
together, including shadowed column files. Adjacent groups are packed using
`sum(max(file_size, source.split.open-file-cost))` as the weight. A bin is drained
when its weight strictly exceeds `target-file-size`; a group that alone exceeds
the target is isolated. A row-ID gap also drains the bin. A drained bin produces
a task only when it contains at least `compaction.min.file-num` physical files.
The ordinary append small-file ratio does not apply.
Data Evolution permits `compaction.min.file-num=1` to force a single-file
rewrite, for example to materialize the current schema.

Each task streams the latest normal-column values into one file covering the
same contiguous row-ID range. Like Java's default normal compaction, write-time
size and row limits do not roll this output; `target-file-size` guides planning.
The minimum and maximum input sequence numbers and per-column maximum sequence
numbers are retained. Blob and Vector files remain attached to their stable row
IDs and are not rewritten by these tasks.

Deleted physical slots are retained. When deletion vectors are enabled, commit
preparation moves their positions from the previous normal anchor files to the
new files. This uses the latest snapshot on every commit attempt, preserving
concurrent deletions and vectors belonging to untouched groups. Global indexes
remain valid because row IDs and values are preserved. Historical snapshots
continue to reference their original data and deletion vectors.

If compaction publishes before a prepared DELETE, that DELETE fails when it
references replaced anchor files or DV indexes. Prepare the DELETE again from
the latest snapshot; replaying the same stale messages does not rebase it.

Concurrent normal-column writes in a selected row-ID range cause commit to fail;
plan again from the latest snapshot. Disjoint appends can commit alongside the
rewrite. Subsets, serialization and idempotent retries use the same compaction
API as append and PK tables. `rows_written` counts physical slots, including
deleted rows; it counts overlapping column versions only once.

Changing storage options does not migrate values in existing files. If selected
normal files still provide a field that the current schema stores in dedicated
files, compaction is rejected before writing or publishing output. Migrate those
historical inline values first; otherwise omitting the field would lose data.
With deletion vectors enabled, one output file is limited to 4,294,967,296
physical row slots so every local position remains deletable by the current
Rust writer. Wider rewrites are rejected before execution; 64-bit deletion-vector
writing is not yet supported.

Dedicated Blob/Vector compaction, splitting large normal files, and materializing
deletions into reassigned row IDs are not provided. Setting
`data-evolution.compaction.split-large-files=true` or
`data-evolution.compaction.rewrite-row-ids=true` is rejected.

## Current scope

The `full-compaction` changelog producer is unsupported. Primary-key deletion-vector
compaction supports `deduplicate` with `none`, `input` or `lookup`.
Outside Data Evolution mode, row tracking and dedicated
blob/vector storage are unsupported. Query authorization and rowkind fields
are unsupported in all modes. Postpone deletion-vector compaction supports only
`deduplicate` with `none`, `input` or `lookup`, as described above.
Ordered bucketed append, append incremental clustering, primary-key clustering
override (`pk-clustering-override`) and record-level expiration
(`record-level.expire-time`) are also unsupported.
Snapshot index manifests may contain dynamic-bucket HASH indexes and primary-key
deletion vectors, or deletion vectors and global indexes in Data Evolution mode.
Embedded and sidecar file indexes are regenerated according to the captured
table options. Language bindings and
background compaction scheduling are not yet provided.
