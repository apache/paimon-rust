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
    plan = builder.new_incremental_scan(0, 1).plan()
    assert plan.snapshot_id() == 1
    assert pa.Table.from_batches(builder.new_read().read(plan.splits())).to_pydict() == {
        "id": [1], "dt": ["blue"]}
    with pytest.raises(ValueError, match="out of available range"):
        builder.new_incremental_scan(0, 2).plan()


@pytest.mark.parametrize("branch", [None, "blue", "empty"])
def test_tuple_identifier_preserves_dots_and_branch(branch_tables, branch):
    main, _, _ = branch_tables
    root = Path(main.location())
    warehouse = root.parent.parent
    database = warehouse / "namespace.database.db"
    root.parent.rename(database)
    (database / "t").rename(database / "table.with.dots")
    catalog = PaimonCatalog({"warehouse": str(warehouse)})
    name = "table.with.dots" + ("$branch_" + branch if branch else "")
    table = catalog.get_table(("namespace.database", name))
    assert table.branch() == (branch or "main")
    assert table.location() == str(database / "table.with.dots")
    builder = table.new_read_builder()
    plan = builder.new_scan().plan()
    assert plan.snapshot_id() == {None: 2, "blue": 1, "empty": None}[branch]
    if branch != "empty":
        rows = pa.Table.from_batches(builder.new_read().read(plan.splits()))
        assert sorted(rows.column("id").to_pylist()) == ([1] if branch else [1, 2])


@pytest.mark.parametrize("identifier", [
    ("", "t"), ("db", ""), ("db", "t$snapshots"), ("db", "t$branch_../escape"),
    ("db", "t$branch_"), ("db",), ("db", "t", "extra"),
])
def test_tuple_identifier_validation(tmp_path, identifier):
    catalog = PaimonCatalog({"warehouse": str(tmp_path)})
    with pytest.raises(ValueError):
        catalog.get_table(identifier)


@pytest.mark.parametrize("identifier", [("db", 1), None, 1])
def test_invalid_identifier_types(tmp_path, identifier):
    catalog = PaimonCatalog({"warehouse": str(tmp_path)})
    with pytest.raises(TypeError):
        catalog.get_table(identifier)


def test_expire_snapshots(tmp_path):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.edb")
    ctx.sql("CREATE TABLE paimon.edb.t (id INT, name STRING)")
    ctx.sql("INSERT INTO paimon.edb.t VALUES (1, 'a')")
    for i in range(2, 6):
        ctx.sql(f"INSERT OVERWRITE paimon.edb.t VALUES ({i}, 'v{i}')")
    table = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("edb.t")
    ctx.sql("CALL sys.create_tag(table => 'edb.t', tag => 't2', snapshot_id => 2)")
    assert [s.id() for s in table.list_snapshots()] == [5, 4, 3, 2, 1]

    # Every snapshot is recent, so the default `snapshot.time-retained` keeps them.
    assert table.expire_snapshots() == 0

    assert table.expire_snapshots(retain_max=3, retain_min=1) == 2
    assert [s.id() for s in table.list_snapshots()] == [5, 4, 3]

    assert table.expire_snapshots(older_than_ms=2**62, retain_min=1, max_deletes=1) == 1
    assert [s.id() for s in table.list_snapshots()] == [5, 4]

    def ids(builder):
        batches = builder.new_read().read(builder.new_scan().plan().splits())
        return sorted(pa.Table.from_batches(batches).column("id").to_pylist())

    assert ids(table.new_read_builder()) == [5]
    # The tagged snapshot keeps its data files.
    assert ids(table.new_read_builder({"scan.tag-name": "t2"})) == [2]

    with pytest.raises(ValueError, match="must not be less than"):
        table.expire_snapshots(retain_max=1, retain_min=2)
