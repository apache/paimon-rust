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
compaction for Data Evolution tables, and incremental compaction for primary-key
tables with fixed or dynamic buckets. `CompactBuilder` creates
a `CompactPlanner`, a `CompactExecutor`, and a `TableCommit`. Planning and
execution do not publish snapshots; `commit_compact` atomically publishes the
selected results in a `COMPACT` snapshot.

The builder selects a dedicated append, primary-key or Data Evolution planner
for the table mode. They share snapshot capture and the public plan/task types;
each planner owns its file eligibility, grouping and task-selection rules.

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
The planner follows Java's `UniversalCompaction` sorted-run selection. Each L0
file is a run; all files at the same positive level form one run. An explicit
compact drains the captured L0 files, including a single L0 file, and extends
the selection through adjacent runs while their sizes are comparable. L1 is
included when necessary to avoid writing L0 output. Large higher-level runs
remain untouched and supply point-lookup history when required.

Without L0 input, sorted-run count, size ratio and size amplification trigger
maintenance. The Java-compatible defaults are `num-sorted-run.compaction-trigger=5`,
`compaction.size-ratio=1` (percent), `compaction.max-size-amplification-percent=200`,
and `num-levels=compaction-trigger + 1`. Output is key ordered at the level below
the first untouched run, or the highest configured level when every run was
selected. The configured level count expands to accommodate existing files.
A bucket containing just one positive-level run needs no rewrite, even if that
run spans multiple files or carries deletion vectors. Append small-file packing
options do not control PK selection. There is no separate full-compact path;
the incremental policy may naturally select every run based on their sizes.

Without deletion vectors, compaction uses each engine's existing merge function
and persists its merge state, including original sequence metadata and synthesized
DELETE records:

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
Without deletion vectors, compound `partial-update` DELETE states with field
aggregators are rejected. In that mode, both reducer engines also reject
compound groups containing retracts
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

## Primary-key deletion vectors

Primary-key tables with `deletion-vectors.enabled=true` use the same incremental
compaction API with `deduplicate`, `partial-update` or `aggregation` and
`changelog-producer=none`, `input` or `lookup`. Schema validation excludes `first-row`
with deletion vectors.

Planning includes L0 files even when normal batch scans hide them. Execution
reads the captured snapshot's vectors and filters physical
row positions before merging keys. This prevents a previously materialized
aggregate from being counted together with the older row it replaced.

The output contains only live merged values, with a known zero delete-row count.
Winning retracts and fully deleted keys produce no output; a task may remove
all its input files without adding a data file. This follows Java's deletion-vector
lookup compaction semantics. When a key has no history in the selected runs,
point lookup searches the untouched levels and materializes the previous value
with its updates. The previous physical position is marked deleted in that
historical file. With
`changelog-producer=lookup`, it also writes the logical changes described below.

Commit atomically removes vector ranges belonging to rewritten files and unions
new position bitmaps into untouched files' vectors. If an affected index also
contains untouched ranges, those vectors are retained when writing its replacement.
Unrelated indexes and the latest dynamic-bucket HASH entries are retained.
Historical snapshots retain their original data and vector metadata.

A concurrent change to an input or lookup dependency's deletion-vector reference rejects the
commit, including a new vector on a previously undeleted file. Plan again from
the latest snapshot to avoid resurrecting deleted rows. Independent L0 appends,
new vectors on those appends, and changes in other buckets can commit alongside
the rewrite. Serialized results, task subsets and idempotent retries follow the
same rules as ordinary PK compaction.

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
merged result's metadata. `first-row` emits only inserts for previously unseen
keys. `partial-update` and `aggregation` use their existing reducers and state
preservation restrictions. Deletion vectors are applied before history selection
and merging, with deleted keys omitted from DV data output.

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

The executor streams both outputs and aborts both writers on execution failure.
Prepared results include both file lists for serialization, subset commits and
idempotent retries. A concurrent positive-level addition in a selected bucket
rejects the commit because it may change the changelog's history. Concurrent L0
appends remain eligible for a later compaction and produce no premature events.

### Local Lookup File cache

DV compaction, lookup changelog production and `first-row` require
`local-cache.enabled=true` on the catalog's FileIO. Setting this flag only in
table properties does not attach a cache and is rejected when creating an
executor. Ordinary PK compaction without these features does not require a cache.
Lookup visits positive levels from lowest to highest, uses
file key bounds to select a candidate, and stops at the first matching physical
key, including a tombstone. A missing key continues to the next level. Rows
deleted by the captured snapshot's DV are skipped. A historical key rejected by
`first-row` stays in its untouched file; no duplicate data row is written.

The first lookup into an uncached file streams that immutable file once into a
local SST key/position index and Arrow IPC value blocks. Later lookups use the
local index and decoded-block LRU; they do not scan the source again. Bloom
filters avoid unnecessary local reads. Complete KV state, sequence metadata and
physical row positions are retained. Snapshot DVs are applied after lookup, so
the same physical cache can be reused across snapshots with different vectors.
Cache identities include storage namespace, physical file metadata and schema;
a level-only change does not invalidate the bytes.

```rust
let mut options = Options::new();
options.set(CatalogOptions::WAREHOUSE, "s3://bucket/warehouse");
options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
options.set(CatalogOptions::LOCAL_CACHE_DIR, "/local/paimon-cache");
options.set(CatalogOptions::LOCAL_CACHE_MAX_SIZE, "20 GiB");
let catalog = CatalogFactory::create(options).await?;
let table = catalog.get_table(&identifier).await?;
let executor = table.new_compact_builder().new_executor()?;
let cache_metrics = executor.lookup_cache_metrics();
```

Generated SST and value blocks use the existing `local-cache` backend and share
its directory, byte accounting, CRC validation and LRU with ordinary cached
reads. `local-cache.max-size` bounds their combined encoded bytes on disk, or
payload bytes for the memory backend. There is no independent Lookup File
directory or disk quota; `lookup.cache-max-disk-size` is rejected. Configure
`local-cache.dir` to use disk; without it, enabled local-cache uses memory.
The remote-read whitelist does not disable generated Lookup File blocks.
They use the regular LRU, preserving the existing protection for metadata and
global-index reads. `local-cache.shared=true` also shares generated blocks
through the existing shared disk backend.

Completed files survive executor recreation, and the disk backend can reopen
them after its cache manager restarts. Concurrent misses in one lookup manager
build a file once. Each build uses immutable versioned block names and publishes
its descriptor last. Incomplete builds have no published descriptor;
partial blocks remain subject to the same bounded LRU. Evicted, corrupt or
incompatible blocks cause a rebuild from the source file, with one retry per
lookup. Table data and snapshots never depend on disposable cache blocks.

| Option | Default | Behavior |
| --- | --- | --- |
| `lookup.cache-max-memory-size` | `256mb` | Shared budget for local index metadata, SST blocks and decoded value blocks. |
| `lookup.cache-file-retention` | `1h` | Idle retention for loaded lookup readers; recovered descriptors older than this are rebuilt. |
| `lookup.cache-spill-compression` | `zstd` | SST compression: `zstd`, `lz4`, `lzo` or `none`. |

Loaded readers are retained while in use; backend blocks can still be evicted
by other cache users. A block that cannot be retained causes `ResourceExhausted`
without publishing compact output. Repeated misses also fail the task; increase
`local-cache.max-size` to retain the needed Lookup Files. Increase the decoded
memory budget or reduce `read.batch-size` for large value blocks. Source
reads and lookup DV state also use the task's resource context. Metrics expose
`file_builds`, `file_hits` and `file_evictions` (expired or invalidated lookup
readers, rather than all backend block evictions). The cache is disposable and uses
a Rust-private versioned format; table snapshots never depend on its paths.
Java's remote prebuilt `.lookup` files and a background compaction scheduler are
not implemented.

## Dynamic buckets

Dynamic bucket compaction (`bucket=-1`) rewrites each existing partition/bucket
without changing key assignments. HASH index files and their complete assignment
history, including deleted keys, are retained. A concurrent append may replace
the HASH index; compaction carries forward the latest manifest at commit time.
Cross-partition dynamic buckets are also supported: subsequent writers rebuild
their key-location index from the compacted data as usual. Compaction does not
rebalance buckets or reclaim HASH index capacity.

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

The `full-compaction` changelog producer and full compact are unsupported;
`lookup` is supported through incremental primary-key compaction.
Outside Data Evolution mode, row tracking and dedicated blob/vector storage
are unsupported. Deletion vectors are supported for primary-key compaction;
ordinary append deletion-vector compaction is unsupported. Query authorization,
rowkind fields and postpone buckets are unsupported in all modes.
Ordered bucketed append, append incremental clustering, primary-key clustering
override (`pk-clustering-override`) and record-level expiration
(`record-level.expire-time`) are also unsupported.
Snapshot index manifests may contain dynamic-bucket HASH indexes and primary-key
deletion vectors, or deletion vectors and global indexes in Data Evolution mode.
Embedded and sidecar file indexes are regenerated according to the captured
table options. Language bindings and
background compaction scheduling are not yet provided.
