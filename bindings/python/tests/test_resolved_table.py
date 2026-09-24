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


def test_catalog_schema_copy_replaces_options_and_keeps_branch(resolved_source):
    root, schema = resolved_source
    original = Table.from_resolved_schema(str(root), json.dumps(schema))
    schema["options"]["scan.snapshot-id"] = "1"
    historical = original.copy_with_resolved_schema(json.dumps(schema))
    assert _read(historical) == (1, [{"id": 1, "name": "a"}])
    schema["options"].pop("scan.snapshot-id")
    schema["fields"][1]["name"] = "renamed"
    schema["id"] = 1
    resolved = historical.copy_with_resolved_schema(json.dumps(schema))
    assert _read(resolved, {"method": "equal", "field": "renamed", "literals": ["b"]}) == (
        2, [{"id": 2, "renamed": "b"}])
    assert _read(historical)[0] == 1
    branch_root = root / "branch" / "branch-dev"
    (branch_root / "snapshot").mkdir(parents=True)
    shutil.copy(root / "snapshot" / "snapshot-1", branch_root / "snapshot" / "snapshot-1")
    # No branch schema file: the catalog has already provided the complete schema.
    branch = resolved.copy_with_resolved_schema(json.dumps(schema), branch="dev")
    assert branch.branch() == "dev"
    assert branch.new_read_builder().new_scan().plan().snapshot_id() == 1
    assert branch.copy_with_resolved_schema(json.dumps(schema)).branch() == "dev"
    assert branch.copy_with_resolved_schema(json.dumps(schema), branch="main").latest_snapshot().id() == 2


@pytest.mark.parametrize("schema_json", ["{", "{}"])
def test_catalog_schema_copy_rejects_invalid_json(resolved_source, schema_json):
    root, schema = resolved_source
    table = Table.from_resolved_schema(str(root), json.dumps(schema))
    with pytest.raises(ValueError, match="Invalid table schema JSON"):
        table.copy_with_resolved_schema(schema_json)


def test_catalog_schema_copy_validates_branch_and_structure(resolved_source):
    root, schema = resolved_source
    table = Table.from_resolved_schema(str(root), json.dumps(schema))
    with pytest.raises(ValueError):
        table.copy_with_resolved_schema(json.dumps(schema), branch="../escape")
    schema["fields"][1]["id"] = schema["fields"][0]["id"]
    with pytest.raises(ValueError):
        table.copy_with_resolved_schema(json.dumps(schema))


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("object_name", ["t", "t$branch_dev"])
def test_resolved_rest_response_keeps_snapshot_and_token_refresh(
    resolved_source, external, object_name
):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    root, schema = resolved_source
    snapshot = json.loads((root / "snapshot" / "snapshot-1").read_text())
    requests = []
    token_requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path.endswith('/token'):
                token_requests.append(self.path)
                # Expire the first token to verify that subsequent FileIO refreshes it.
                response = {"token": {}, "expiresAtMillis": (
                    0 if len(token_requests) == 1 else 4102444800000)}
            elif self.path.endswith('/snapshot'):
                # Disk has snapshot 2; REST snapshot 1 must remain authoritative.
                response = {"snapshot": {"snapshot": snapshot}}
            else:
                self.send_error(500, "Unexpected metadata request")
                return
            body = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # PyPaimon and older REST servers do not include `database`.
        response = {"id": "table-uuid", "name": object_name, "path": str(root),
                    "isExternal": external, "schemaId": schema['id'], "schema": schema}
        table = Table.from_rest_response(
            json.dumps(response),
            database='db',
            table=object_name,
            rest_options={
                'uri': 'http://127.0.0.1:%d' % server.server_port,
                'warehouse': 'test', 'token.provider': 'bear', 'token': 'test-token',
                'data-token.enabled': 'true',
            },
        )
        assert table.branch() == ('dev' if '$branch_' in object_name else 'main')
        assert len(token_requests) == (0 if external else 1)
        assert all(path.endswith('/token') for path in requests)
        assert _read(table) == (1, [{'id': 1, 'name': 'a'}])
        assert all(path.endswith(('/token', '/snapshot')) for path in requests)
        encoded_name = object_name.replace('$', '%24')
        assert any(path.endswith(
            f'/databases/db/tables/{encoded_name}/snapshot') for path in requests)
        assert len(token_requests) == (0 if external else 2)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize(("database", "table"), [("db", "wrong"), ("wrong", "t")])
def test_resolved_rest_response_rejects_identity_mismatch(resolved_source, database, table):
    root, schema = resolved_source
    response = {"id": "table-uuid", "database": "db", "name": "t", "path": str(root),
                "isExternal": True, "schemaId": schema["id"], "schema": schema}
    # Identity validation must run before REST auth/cache initialization.
    with pytest.raises(ValueError, match="does not match requested identifier"):
        Table.from_rest_response(
            json.dumps(response), database=database, table=table, rest_options={})


@pytest.mark.parametrize(("change", "message"), [
    ("duplicate_id", "duplicate field id"),
    ("missing_primary_key", "primary key"),
    ("missing_partition_key", "partition fields"),
])
def test_resolved_rest_response_validates_schema_structure(resolved_source, change, message):
    root, schema = resolved_source
    if change == "duplicate_id":
        schema["fields"][1]["id"] = schema["fields"][0]["id"]
    elif change == "missing_primary_key":
        schema["primaryKeys"] = ["missing"]
    else:
        schema["partitionKeys"] = ["missing"]
    response = {"id": "table-uuid", "database": "db", "name": "t", "path": str(root),
                "isExternal": True, "schemaId": schema["id"], "schema": schema}
    with pytest.raises(ValueError, match=message):
        Table.from_rest_response(json.dumps(response), database="db", table="t", rest_options={
            "uri": "http://127.0.0.1:1", "warehouse": "test",
            "token.provider": "bear", "token": "test-token",
        })


@pytest.mark.parametrize('response', ['{', '{}'])
def test_resolved_rest_response_rejects_missing_metadata(response):
    with pytest.raises(ValueError):
        Table.from_rest_response(response, database='db', table='t', rest_options={
            'uri': 'http://127.0.0.1:1', 'warehouse': 'test',
            'token.provider': 'bear', 'token': 'test-token',
        })
