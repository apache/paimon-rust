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

# Streaming Reading

`StreamTableScan` reads a table continuously: an optional full state first, then
the changes of each later snapshot. Filters and projection configured on the
read builder apply to every plan.

## Read Loop

`plan()` returns the next plan, or `None` when no new snapshot is available
yet. `checkpoint()` is the next snapshot to read. Acknowledge it only after the
plan's rows have been processed:

```rust
use futures::TryStreamExt;
use std::time::Duration;

let read_builder = table.new_read_builder();
let mut scan = read_builder.new_stream_scan()?;
let read = read_builder.new_read()?;
loop {
    let Some(plan) = scan.plan().await? else {
        tokio::time::sleep(Duration::from_secs(1)).await;
        continue;
    };
    let batches = read
        .to_arrow_with_row_kind(plan.splits())?
        .try_collect::<Vec<_>>()
        .await?;
    // Process `batches`, then acknowledge the progress.
    scan.notify_checkpoint_complete(scan.checkpoint()).await?;
}
```

`notify_checkpoint_complete` persists the progress when the scan has a consumer,
set by table option `consumer-id` or `StreamTableScan::with_consumer_id`. A new
scan with the same consumer resumes from it. `restore(Some(id))` resumes from a
checkpoint kept by the caller, and `restore(None)` starts over from consumer
progress or `scan.mode`.

## Startup Modes

An explicit checkpoint takes precedence over consumer progress, and both take
precedence over `scan.mode`. Without either, the first plan starts from
`scan.mode`:

| `scan.mode` | Required option | First plan |
|-------------|-----------------|------------|
| `latest-full` (default) | | Full state of the latest snapshot |
| `latest` | | Changes after the snapshot that is latest at the first plan; an empty table streams from its first snapshot |
| `from-snapshot` | `scan.snapshot-id` | Changes from that snapshot, without a full state |
| `from-snapshot-full` | `scan.snapshot-id` | Full state of that snapshot |
| `from-timestamp` | `scan.timestamp-millis` or `scan.timestamp` | Changes from the first snapshot committed at or after that time |

Later plans read the changes of each following snapshot. Without `scan.mode`,
`scan.snapshot-id` selects `from-snapshot`, and `scan.timestamp-millis` or
`scan.timestamp` selects `from-timestamp`, as in Java.

On an empty table, every mode waits for the first commit. A snapshot ID below
the earliest snapshot starts from the earliest snapshot. With `from-snapshot`,
the ID after the latest snapshot waits for the next commit, and a larger ID
fails after some empty polls. With `from-snapshot-full`, once the table has a
snapshot, each plan fails with `SnapshotNotExist` until the selected one
exists. A timestamp only selects the first snapshot: when it is later than
every snapshot, the stream starts at the next commit, whatever its commit time,
rather than waiting until then.

```rust
use std::collections::HashMap;

let table = table.copy_with_options(HashMap::from([
    ("scan.mode".to_string(), "from-snapshot".to_string()),
    ("scan.snapshot-id".to_string(), "5".to_string()),
]));
let mut scan = table.new_stream_scan()?;
```

The starting position is resolved only when the scan starts from `scan.mode`.
There, the first plan fails for options a stream cannot honor: `scan.tag-name`,
`scan.watermark`, `scan.version`, `incremental-between-timestamp`,
`scan.file-creation-time-millis`, and `scan.creation-time-millis`. Every plan
still validates the scan options, so an invalid or unsupported configuration,
such as `from-snapshot-full` without `scan.snapshot-id` or
`scan.mode=compacted-full`, also fails after `restore`.

## Row Kinds

`TableRead::to_arrow_with_row_kind` adds a leading `rowkind` column:

- A full state contains the merged live rows of its snapshot as `+I`. It does
  not replay earlier updates or deletes.
- Later plans keep the physical events of their files: `+I`, `-U`, `+U`, and
  `-D`. Without a changelog producer, they come from the data files of `APPEND`
  snapshots; otherwise, from changelog files.

With `changelog-producer=none` or `input`, the full state merges every level,
including uncompacted level-0 files of deletion-vector tables. With `lookup`,
the full state skips level 0; with `full-compaction`, it reads only the last
level. Rows in the other levels arrive later as changelog from compaction.

## Limitations

- Long-lived changelogs are not read. Restoring a checkpoint older than the
  earliest snapshot fails.
- `scan.bounded.watermark`, `streaming.read.snapshot.delay`, and
  `consumer.ignore-progress` are not supported.
