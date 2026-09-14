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

import shutil
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


@pytest.fixture
def branch_tables(tmp_path):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.bdb")
    ctx.sql("CREATE TABLE paimon.bdb.t (id INT, dt STRING) PARTITIONED BY (dt)")
    ctx.sql("INSERT INTO paimon.bdb.t VALUES (1, 'blue')")
    ctx.sql("INSERT INTO paimon.bdb.t VALUES (2, 'main')")
    catalog = PaimonCatalog({"warehouse": str(tmp_path)})
    main = catalog.get_table("bdb.t")
    root = Path(main.location())

    # Build branch metadata from actual committed schema/snapshot files, matching
    # BranchManager.create_branch_from_tag. Manifests and data remain shared.
    for branch in ("blue", "empty"):
        branch_root = root / "branch" / ("branch-" + branch)
        (branch_root / "schema").mkdir(parents=True)
        shutil.copy(root / "schema" / "schema-0", branch_root / "schema" / "schema-0")
    blue = root / "branch" / "branch-blue"
    (blue / "snapshot").mkdir()
    shutil.copy(root / "snapshot" / "snapshot-1", blue / "snapshot" / "snapshot-1")
    (blue / "tag").mkdir()
    shutil.copy(root / "snapshot" / "snapshot-1", blue / "tag" / "tag-blue")
    ctx.sql("CALL sys.create_tag(table => 'bdb.t', tag => 'main', snapshot_id => 2)")
    return main, catalog.get_table("bdb.t$branch_blue"), catalog.get_table("bdb.t$branch_empty")


def test_branch_identity_and_read(branch_tables):
    main, blue, empty = branch_tables
    assert main.branch() == "main"
    assert blue.branch() == "blue"
    assert empty.branch() == "empty"
    for table, expected in ((main, [1, 2]), (blue, [1])):
        builder = table.new_read_builder()
        plan = builder.new_scan().plan()
        batches = builder.new_read().read(plan.splits())
        assert sorted(pa.Table.from_batches(batches).column("id").to_pylist()) == expected
    assert empty.new_read_builder().new_scan().plan().snapshot_id() is None


def test_branch_snapshots_are_isolated(branch_tables):
    main, blue, empty = branch_tables
    assert main.latest_snapshot().id() == 2
    assert blue.latest_snapshot().id() == 1
    assert empty.latest_snapshot() is None
    assert [s.id() for s in main.list_snapshots()] == [2, 1]
    assert [s.id() for s in blue.list_snapshots()] == [1]
    assert empty.list_snapshots() == []


def test_branch_tags_are_isolated(branch_tables):
    main, blue, empty = branch_tables
    assert [(t.name(), t.snapshot_id()) for t in main.list_tags()] == [("main", 2)]
    assert [(t.name(), t.snapshot_id()) for t in blue.list_tags()] == [("blue", 1)]
    assert empty.list_tags() == []
    plan = blue.new_read_builder({"scan.tag-name": "blue"}).new_scan().plan()
    assert plan.snapshot_id() == 1
    with pytest.raises(ValueError, match="main"):
        blue.new_read_builder({"scan.tag-name": "main"})


@pytest.mark.parametrize("suffix", [
    "$branch_missing", "$branch_", "$branch_../escape", "$snapshots", "$branch_blue$snapshots",
])
def test_invalid_branch_and_system_table_identifiers_do_not_read_main(branch_tables, suffix):
    main, _, _ = branch_tables
    catalog = PaimonCatalog({"warehouse": str(Path(main.location()).parent.parent)})
    with pytest.raises(ValueError):
        catalog.get_table("bdb.t" + suffix)


def test_branch_partition_statistics_are_isolated(branch_tables):
    main, blue, empty = branch_tables
    assert sorted(p["dt"] for p in main.list_partitions()) == ["blue", "main"]
    assert blue.list_partitions() == [{"dt": "blue"}]
    assert empty.list_partitions() == []
    assert sum(p.record_count() for p in main.partition_stats()) == 2
    stats = blue.partition_stats()
    assert len(stats) == 1
    assert stats[0].partition() == {"dt": "blue"}
    assert stats[0].record_count() == 1
    assert stats[0].file_count() == 1
    assert empty.partition_stats() == []


def test_branch_incremental_scan_uses_branch_snapshot_bounds(branch_tables):
    _, blue, _ = branch_tables
    builder = blue.new_read_builder()
    plan, trace = builder.new_incremental_scan(0, 1).plan_with_trace()
    assert plan.snapshot_id() == trace["snapshot_id"] == 1
    assert pa.Table.from_batches(builder.new_read().read(plan.splits())).to_pydict() == {
        "id": [1], "dt": ["blue"]}
    with pytest.raises(ValueError, match="out of available range"):
        builder.new_incremental_scan(0, 2).plan()
