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

Writers also run one synchronous compaction round after a successful data commit.
`write-only` defaults to `false`; set it to `true` to leave compaction to a
dedicated job. The legacy `write.compaction-skip` alias is accepted, with
`write-only` taking precedence. Automatic compaction uses the existing planner's
thresholds and selects eligible files from the latest snapshot on the same branch,
restricted to the partitions and buckets changed by this write. It uses the current
schema even when the writer retains a historical schema, including schemas resolved
through a REST catalog. Primary-key writes honor normal sorted-run count and size
triggers. Lookup, first-row and deletion-vector compaction, or an explicit
`compaction.force-up-level-0=true`, can advance L0 files below those triggers.
Overwrite compaction is pinned to its own committed data snapshot, so suppressing
overwrite changelogs cannot consume a later append's pending lookup events.
As in Java, overwriting partitions being consumed by streaming readers is unsupported;
lookup changelogs do not describe the overwrite operation itself.
Automatic tasks share one captured snapshot and bucket-grouped source/index
metadata per round. Automatic index fields also share captured source metadata;
native builds only plan files overlapping missing or selected merge ranges.
Covered fields can still consolidate runs when their thresholds are met.
Captured metadata is released after the round. Publication and retries still
validate current sources and index
membership, and manual task execution captures fresh inputs on every call.
Automatic lookup compaction supports the default `lookup-compact=radical` policy.
`gentle` requires writer-lifetime trigger intervals and is explicitly unsupported
here; use `write-only=true` and schedule dedicated compaction separately.
An empty plan publishes no extra snapshot. Append, overwrite and state-dependent
data commits use this behavior; empty commits and truncation do not trigger it.
`write.compaction.parallelism` bounds concurrent buckets within one automatic
round (default `4`, positive integer; `1` runs serially). Tasks in the same
partition and bucket run in order. The commit waits for the complete round and
publishes all successful results together. On execution failure, queued buckets
are skipped, started buckets finish, and their unpublished outputs are cleaned up.
Normal commits pass their actual data snapshot and touched bucket set directly
to compaction; only recovery reconstructs them from stored snapshots and manifests.
Table modes unsupported by the compaction API retain their write behavior without
automatic compaction. Automatic lookup, first-row and deletion-vector compaction
use the configured catalog cache when available, otherwise a temporary disk cache
owned by the writer's compaction work. `lookup.cache-max-disk-size` bounds that
temporary cache when configured. Temporary files remain alive until the last cache
reader is released. Explicit manual compaction still requires
`local-cache.enabled=true` on the catalog/FileIO for these modes.
For plain partial-update and aggregation tables, an unsupported lossless retract
rewrite also leaves the raw source files in place. I/O, resource and data errors,
and lookup changelog failures, are still returned to the caller.

Data publication and compaction are separate transactions. A compaction failure
leaves the data committed and returns an error retaining the underlying cause.
For recoverable writes, use a stable commit user and caller-provided identifier,
then retry with `filter_and_commit_with_identifier` (or `filter_and_commit`) for
append, or `overwrite_with_identifier` for overwrite.
Recovery finishes an interrupted compaction round without repeating the data
commit or a completed automatic `COMPACT` snapshot. A single round can leave
additional work for a later write or a dedicated compaction job. This option does
not schedule background work or expire snapshots.

## Automatic global index maintenance

With `write-only=false` (the default), writes synchronously maintain declared
global indexes after automatic compaction. An empty compaction plan still allows
index maintenance. `write-only=true` leaves both jobs to dedicated operators.
Rust, Python, and DataFusion writes share this behavior. Tables without index
declarations retain their existing write behavior; no separate index switch is
required and no background job is scheduled.

Index options follow the primary-key index convention: a comma-separated column
list for each family and optional per-column parameters. Supported families are
`btree`, `bitmap`, `multivalue`, `fm`, `full-text`, and `vector`:

```rust
let options = std::collections::HashMap::from([
    ("row-tracking.enabled".into(), "true".into()),
    ("bucket".into(), "-1".into()),
    ("write-only".into(), "false".into()),
    ("global-index.btree.index-columns".into(), "id,age".into()),
    ("global-index.full-text.index-columns".into(), "text".into()),
    (
        "fields.text.global-index.full-text.index-options".into(),
        r#"{"lower-case":true}"#.into(),
    ),
    ("global-index.vector.index-columns".into(), "embedding".into()),
    ("fields.embedding.global-index.vector.index-type".into(), "diskann".into()),
    ("fields.embedding.global-index.vector.distance-metric".into(), "cosine".into()),
    (
        "fields.embedding.global-index.vector.index-options".into(),
        r#"{"dimension":768}"#.into(),
    ),
]);
```

Maintenance reuses the global-index builders and their table, column-type,
and native writer restrictions. Tables must have row tracking enabled, no
primary key, and `bucket=-1`. Data evolution and the query-side
`global-index.enabled` option are not prerequisites for building indexes.
Full-text builds additionally reject deletion-vector tables. Declared columns
are trimmed; empty names, duplicates, unknown fields, and assigning multiple
families to one column are rejected before data publication. Each listed column
has its own index. Use the explicit build API for composite BTree indexes.
Update the lists and per-column option keys together with schema renames or drops.
`write-only=true` skips automatic index configuration validation.

Per-column parameters use
`fields.<column>.global-index.<family>.index-options`, a JSON object containing
string, boolean, or numeric values. Short keys are qualified with the native
writer prefix, such as `block-size` to `btree-index.block-size`. Fully qualified
writer keys and shared shard options are accepted. Conflicting native writer
values are rejected, matching primary-key option resolution. Vector columns
additionally require `fields.<column>.global-index.vector.index-type`; the optional
`distance-metric` defaults to `inner_product`.
Explicit single-column builds also use these declarations as defaults; explicit
builder options take precedence over them.
FULL/DETAIL raw fallback uses the same per-column parameters as automatic
builds, including vector metric defaults and full-text tokenization. A declared
full-text column can use raw fallback before its first index is published.

Each round captures the current schema and one source snapshot. Missing row-ID
coverage is built as independent index files, retaining compatible existing
payloads. Adjacent index ranges form runs within one partition, bucket and row-ID
shard; logical row-ID holes are separate groups. The UniversalCompaction run
selector is shared with PK data compaction. Global index runs use logical row
counts as their size measure, so new ranges can participate in selection before
encoding; PK data runs continue to use bytes and their existing level rules.

Index merging has independent options with the same defaults as PK compaction:

| Option | Default | Meaning |
| --- | --- | --- |
| `global-index.num-sorted-run.compaction-trigger` | `5` | Run count at which size-based selection becomes eligible; must be positive. |
| `global-index.compaction.size-ratio` | `1` | Percentage tolerance when extending a merge to the next run; must be nonnegative. |
| `global-index.compaction.max-size-amplification-percent` | `200` | Select all runs when newer runs exceed this percentage of the oldest run; must be nonnegative. |

These options can also appear, fully qualified, in per-column `index-options`
JSON and override the corresponding table-level index policy. They do not change
the table's data-compaction options. Above the run-count limit, the selector
also makes progress when adjacent run sizes differ too much
for the normal size ratio. A single run is never rebuilt just to compact itself.

For example, with a 1000-row-ID shard, an existing `[0, 599]` index and a new
`[600, 799]` batch stay as two files below the trigger. Once enough adjacent runs
accumulate, selected ranges are rebuilt together from current data and sidecar
values. A large older run can remain untouched while smaller tail runs merge.
The implementation rebuilds selected data ranges rather than concatenating old
native index payloads. A physical compaction preserving row IDs and values can
retain global payloads. Shard-boundary changes, removed rows and incompatible
vector definitions retire affected payloads immediately. Additions and retired
references publish together; historical snapshots retain their old files.

Scalar and full-text payload reuse also checks the indexed field's type. New file
names record the actual builder schema ID; this remains valid after its source
snapshot expires and permits current-schema builds over older data. Java opens
the manifest's file name unchanged, so native and manifest formats are unchanged.
Legacy names require a complete schema history with an unchanged field ID and
type. Missing history or incompatible types cause atomic retirement and rebuild;
scalar layout-option changes alone retain compatible runs. Full-text type
changes, such as a shorter VARCHAR width, rebuild tokens from current logical
text. Full-text runs also require an equivalent parsed native configuration;
analyzer changes rebuild coverage, while explicit defaults preserve compatible
leaves. Missing or invalid analyzer metadata is rebuilt conservatively.

Vector training still uses the actual non-NULL count. If a small or sparse run
cannot satisfy its native training configuration, such as a size-per-vector
budget, maintenance may retry once with complete pending connected groups. This
keeps the declared algorithm, metric and budget. I/O and training execution
errors do not trigger this expansion. All-NULL vector builds retain Java's
existing empty-build behavior: they publish no zero-byte DE index marker.
Manual `commit_compact` also maintains declared indexes when `write-only=false`.
An empty maintenance plan publishes no snapshot. Empty data commits, index-only
commits, and truncation do not trigger a separate round. Indexed-column updates
retain `global-index.column-update-action`: use `DROP_PARTITION_INDEX` to allow
the data update and automatically rebuild the affected coverage. Changing a
vector algorithm, distance metric or dimension retires all incompatible payloads
for that column and publishes their replacements together. Other vector training
parameters alone do not rebuild unchanged coverage; changing shard boundaries
can regroup it.
Source vectors must satisfy a changed dimension. If they do not, maintenance
fails without publishing mixed-dimension payloads; fix the source values and
retry maintenance with the same originating commit identity.
Missing coverage is processed synchronously without a minimum-row or time
delay; index consolidation follows the run thresholds above.

A concurrent snapshot or schema change causes private outputs to be discarded
and coverage to be replanned, bounded by `commit.max-retries` and
`commit.timeout`. A stable identity derived from the originating data commit
makes repeated recovery idempotent. Uncertain publication outcomes retain
submitted files and check for an already published index snapshot before
retrying. Data, compaction, and index publication are separate transactions:
global row IDs must be assigned before indexing. An index failure leaves the
data committed and returns an error preserving the
cause. Retry append with `filter_and_commit_with_identifier` or
`filter_and_commit`, or overwrite with `overwrite_with_identifier`, using the
same commit user and identifier. Recovery resumes indexing even when automatic
compaction already completed. For an interrupted manual compaction, retry the
same prepared results and commit user through `commit_compact`.

## Compaction planning

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

Primary-key compaction also maintains declared source-backed indexes, using
Java's primary-key options:

| Option | Meaning |
| --- | --- |
| `pk-btree.index.columns` | Comma-separated BTree columns. |
| `pk-bitmap.index.columns` | Comma-separated bitmap columns. |
| `pk-multivalue.index.columns` | Comma-separated array columns. |
| `pk-fm.index.columns` | Comma-separated FM string columns. |
| `pk-full-text.index.columns` | Comma-separated full-text string columns. |
| `pk-vector.index.columns` | Vector column, subject to the existing PK vector constraints. |
| `fields.<column>.pk-<family>.index.options` | JSON object of native writer options. |
| `fields.<column>.pk-vector.index.type` | Vector algorithm, such as `ivf-flat`. |
| `fields.<column>.pk-vector.distance.metric` | Vector metric: `l2`, `cosine`, or `inner_product`. |

Declarations are unset by default. With `write-only=false`, the normal automatic
compaction round maintains them. Explicit compaction also maintains them with
`write-only=true`, for dedicated compactors. This uses the same native writers
as global indexes. PK vector and full-text search retain their existing
deletion-vector and search-mode requirements.

Each payload covers the complete compacted file set for one partition, bucket,
positive level and column. Source files are sorted by name; payload ordinals
concatenate their physical rows, preserving NULL and tombstone positions.
Each native build projects its indexed column rather than decoding every value
column. As in Java, vector builds exclude captured DV positions and lookup
deletions discovered by the current compaction, while keeping their physical
positions in the source frame. Selection precedes schema casts; bounded NULL
batches fill ordinal gaps for the shared native writer. Scalar and full-text
builds retain physical rows.
Unchanged coverage is retained. Changed coverage and incompatible vector
definitions replace the previous references in the same COMPACT snapshot as
data, changelogs and deletion vectors. Old payload files remain for historical
snapshots. L0 files remain outside these indexes and use the existing read path.
All-NULL vector levels retain an explicit empty segment, as in Java, so later
maintenance reuses their coverage without rescanning or opening a native index.
Vector payload row counts count indexed vectors; source ranges retain all physical positions.

Commit validates the complete source set and captured index membership before
publishing. Concurrent L0 appends and unrelated index changes can be rebased;
changes to a maintained unit require a new plan.
For newly built vector payloads, captured deleted positions must remain deleted
at commit. Concurrent DV additions and container repacking can be rebased;
restoring positions, for example through rollback-as-latest, requires re-execution.
A failed build cleans its private outputs. A failed or uncertain commit retains
prepared outputs for retry with the same results and commit user. Older serialized results lacking
index maintenance must be re-executed before first publication when indexes
need maintenance; already published results remain recoverable without their
original source snapshot.
As in Java, declared indexed columns cannot be renamed, dropped or have their
type changed while the declaration remains in effect.
If a declaration is removed to change a scalar column's type, retained files with
different source types use the normal read path until rewritten. Maintenance and
reads share this codec check, including when a type is changed back later.

The same API compacts primary-key tables with `deduplicate`,
`first-row`, `partial-update` or `aggregation` merge engines. Changelog producers
`none`, `input` and `lookup` are supported; `first-row` supports `none` or `lookup`.
For `deduplicate` with `none`, `input` or `lookup`, and non-DV `first-row` with
`none` or `lookup`, the planner chooses incremental sorted runs using Java's
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
blob/vector storage are unsupported. Query authorization, rowkind fields and
postpone buckets are unsupported in all modes.
Ordered bucketed append, append incremental clustering, primary-key clustering
override (`pk-clustering-override`) and record-level expiration
(`record-level.expire-time`) are also unsupported.
Snapshot index manifests may contain dynamic-bucket HASH indexes and primary-key
deletion vectors and source-backed PK indexes, or deletion vectors and global
indexes in Data Evolution mode.
Embedded and sidecar file indexes are regenerated according to the captured
table options. The automatic behavior also applies to language bindings using the
table committer. The manual compaction API and background compaction scheduling
are not yet exposed through language bindings.
