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

import copy
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext, Table


@pytest.fixture
def resolved_source(tmp_path):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    ctx.sql("CREATE TABLE paimon.db.t (id INT, name STRING)")
    ctx.sql("INSERT INTO paimon.db.t VALUES (1, 'a')")
    ctx.sql("INSERT INTO paimon.db.t VALUES (2, 'b')")
    table = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("db.t")
    root = Path(table.location())
    schema = json.loads((root / "schema" / "schema-0").read_text())
    return root, schema


def _read(table, predicate=None):
    builder = table.new_read_builder()
    if predicate is not None:
        builder = builder.with_filter(predicate)
    plan = builder.new_scan().plan()
    batches = builder.new_read().read(plan.splits())
    return plan.snapshot_id(), pa.Table.from_batches(batches).to_pylist() if batches else []


def test_resolved_schema_preserves_field_ids_and_names(resolved_source):
    root, schema = resolved_source
    # An external catalog has resolved a rename; on-disk data still uses `name`.
    # Filters, projection and physical reads must all use the supplied schema.
    schema["id"] = 1
    schema["fields"][1]["name"] = "renamed"
    table = Table.from_resolved_schema(str(root), json.dumps(schema), database="db", table="t")
    assert table.identifier() == "db.t"
    assert table.location() == str(root)
    assert [field.name() for field in table.schema().fields()] == ["id", "renamed"]
    assert _read(table, {"method": "equal", "field": "renamed", "literals": ["b"]}) == (
        2, [{"id": 2, "renamed": "b"}])
    builder = table.new_read_builder().with_projection(["renamed"])
    batches = builder.new_read().read(builder.new_scan().plan().splits())
    assert sorted(pa.Table.from_batches(batches).column("renamed").to_pylist()) == ["a", "b"]


def test_resolved_options_replace_persisted_options(resolved_source):
    root, schema = resolved_source
    persisted = copy.deepcopy(schema)
    persisted["options"]["scan.snapshot-id"] = "1"
    (root / "schema" / "schema-0").write_text(json.dumps(persisted))
    latest = Table.from_resolved_schema(str(root), json.dumps(schema))
    assert _read(latest)[0] == 2
    assert sorted(row["id"] for row in _read(latest)[1]) == [1, 2]
    schema["options"]["scan.snapshot-id"] = "1"
    historical = Table.from_resolved_schema(str(root), json.dumps(schema))
    assert _read(historical) == (1, [{"id": 1, "name": "a"}])
    assert "scan.snapshot-id" not in latest.schema().options()


def test_resolved_branch_snapshot_tag_and_empty_plan(resolved_source):
    root, schema = resolved_source
    branch_root = root / "branch" / "branch-dev"
    (branch_root / "snapshot").mkdir(parents=True)
    shutil.copy(root / "snapshot" / "snapshot-1", branch_root / "snapshot" / "snapshot-1")
    (branch_root / "tag").mkdir()
    shutil.copy(root / "snapshot" / "snapshot-1", branch_root / "tag" / "tag-release")
    # Construction/planning need no catalog or latest schema file in the branch.
    table = Table.from_resolved_schema(str(root), json.dumps(schema), branch="dev")
    assert table.branch() == "dev"
    assert table.latest_snapshot().id() == 1
    assert table.new_read_builder().new_scan().plan().snapshot_id() == 1
    schema["options"]["scan.tag-name"] = "release"
    tagged = Table.from_resolved_schema(str(root), json.dumps(schema), branch="dev")
    plan = tagged.new_read_builder().with_filter(
        {"method": "equal", "field": "id", "literals": [99]}).new_scan().plan()
    assert plan.snapshot_id() == 1
    assert plan.splits() == []
    schema["options"].pop("scan.tag-name")
    empty = Table.from_resolved_schema(str(root), json.dumps(schema), branch="empty")
    assert empty.new_read_builder().new_scan().plan().snapshot_id() is None


def test_resolved_schema_does_not_load_catalog(tmp_path):
    schema = {"version": 3, "id": 0, "fields": [{"id": 0, "name": "id", "type": "INT"}],
              "highestFieldId": 0, "partitionKeys": [], "primaryKeys": [],
              "options": {}, "timeMillis": 0}
    table = Table.from_resolved_schema(tmp_path.as_uri(), json.dumps(schema), options={})
    assert table.new_read_builder().new_scan().plan().snapshot_id() is None


@pytest.mark.parametrize("invalid", ["{", "{}", '{"fields": null}'])
def test_resolved_schema_rejects_invalid_json(tmp_path, invalid):
    with pytest.raises(ValueError, match="Invalid table schema JSON"):
        Table.from_resolved_schema(str(tmp_path), invalid)


@pytest.mark.parametrize("change", ["duplicate_id", "duplicate_name", "missing_pk"])
def test_resolved_schema_validates_structure(resolved_source, change):
    root, schema = resolved_source
    if change == "duplicate_id":
        schema["fields"][1]["id"] = schema["fields"][0]["id"]
    elif change == "duplicate_name":
        schema["fields"][1]["name"] = schema["fields"][0]["name"]
    else:
        schema["primaryKeys"] = ["missing"]
    with pytest.raises(ValueError):
        Table.from_resolved_schema(str(root), json.dumps(schema))


@pytest.mark.parametrize("kwargs", [{"branch": "../escape"}, {"database": ""}, {"table": "../t"}])
def test_resolved_schema_validates_metadata_identity(resolved_source, kwargs):
    root, schema = resolved_source
    with pytest.raises(ValueError):
        Table.from_resolved_schema(str(root), json.dumps(schema), **kwargs)


def test_resolved_file_io_options_require_strings(resolved_source):
    root, schema = resolved_source
    with pytest.raises(TypeError):
        Table.from_resolved_schema(str(root), json.dumps(schema), options={"key": True})


def test_resolved_schema_keeps_query_authorization_guard(resolved_source):
    root, schema = resolved_source
    schema["options"]["query-auth.enabled"] = "true"
    table = Table.from_resolved_schema(str(root), json.dumps(schema))
    builder = table.new_read_builder()
    with pytest.raises(NotImplementedError, match="query-auth"):
        builder.new_scan().plan()
    with pytest.raises(NotImplementedError, match="query-auth"):
        builder.new_read().read([])
