# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


@pytest.mark.parametrize("write_only", [None, "false", "true"])
@pytest.mark.parametrize("mode", [
    "append", "deduplicate", "deduplicate-triggered", "first-row", "lookup", "deletion-vector",
])
def test_batch_auto_compaction_visibility_and_changelog(tmp_path, mode, write_only):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.compactdb")
    options = {}
    primary_key = mode != "append"
    if primary_key:
        options["bucket"] = "1"
    else:
        options["compaction.min.file-num"] = "2"
    if mode == "deduplicate-triggered":
        options["num-sorted-run.compaction-trigger"] = "2"
        options["compaction.max-size-amplification-percent"] = "0"
    elif mode == "first-row":
        options["merge-engine"] = "first-row"
    elif mode == "lookup":
        options["changelog-producer"] = "lookup"
    elif mode == "deletion-vector":
        options["deletion-vectors.enabled"] = "true"
    if write_only is not None:
        options["write-only"] = write_only
    key = ", PRIMARY KEY (id)" if primary_key else ""
    properties = ", ".join(f"'{name}' = '{value}'" for name, value in options.items())
    ctx.sql(f"CREATE TABLE paimon.compactdb.t (id INT, value INT{key}) WITH ({properties})")
    # No local-cache opt-in: first-row, lookup and DV must own their resources.
    table = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("compactdb.t")
    for value in (10, 20):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.record_batch({
            "id": pa.array([1], type=pa.int32()),
            "value": pa.array([value], type=pa.int32()),
        }))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    enabled = write_only != "true"
    compacted = enabled and mode != "deduplicate"
    snapshot_id = table.latest_snapshot().id()
    snapshot = json.loads((Path(table.location()) / "snapshot" / f"snapshot-{snapshot_id}").read_text())
    assert snapshot["commitKind"] == ("COMPACT" if compacted else "APPEND")
    expected_id = 2
    if compacted:
        expected_id += 1 if mode in ("append", "deduplicate-triggered") else 2
    assert snapshot_id == expected_id

    read = table.new_read_builder()
    actual = sorted((row["id"], row["value"])
                    for batch in read.new_read().read(read.new_scan().plan().splits())
                    for row in batch.to_pylist())
    if not enabled and mode in ("first-row", "deletion-vector"):
        expected = []
    elif mode == "append":
        expected = [(1, 10), (1, 20)]
    else:
        expected = [(1, 10 if mode == "first-row" else 20)]
    assert actual == expected

    if mode == "lookup":
        changes = table.new_read_builder().with_include_row_kind(True)
        plan = changes.new_incremental_scan(0, snapshot_id, "auto").plan()
        events = [(row["rowkind"], row["id"], row["value"])
                  for batch in changes.new_read().read(plan.splits()) for row in batch.to_pylist()]
        assert sorted(events) == sorted([
            ("+I", 1, 10), ("-U", 1, 10), ("+U", 1, 20),
        ] if enabled else [])
