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

"""Branch operations use core REST APIs and release the GIL during requests."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote_plus

import pytest

from pypaimon_rust.datafusion import PaimonCatalog


@pytest.fixture
def catalog(monkeypatch):
    requests = []
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1")
    monkeypatch.setenv("no_proxy", "localhost,127.0.0.1")

    class Handler(BaseHTTPRequestHandler):
        def handle_operation(self):
            assert self.headers["Authorization"] == "Bearer test-token"
            size = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(size)) if size else None
            requests.append((self.command, self.path, body))
            parts = [unquote_plus(part) for part in self.path.split("/")]
            if self.path.startswith("/v1/config?"):
                payload, status = {"defaults": {"prefix": "test"}}, 200
            elif "missing" in parts:
                payload, status = {"resourceType": "TABLE", "message": "missing", "code": 404}, 404
            elif "forbidden" in parts:
                payload, status = {"message": "denied", "code": 403}, 403
            elif "conflict" in parts:
                payload, status = {"message": "exists", "code": 409}, 409
            elif self.command == "GET":
                payload, status = {"branches": ["dev", "release"]}, 200
            else:
                payload, status = None, 204
            output = json.dumps(payload).encode() if status != 204 else b""
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)

        do_GET = handle_operation
        do_POST = handle_operation
        do_DELETE = handle_operation

        def log_message(self, *args):
            pass

    server = HTTPServer(("localhost", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        native = PaimonCatalog({
            "metastore": "rest", "uri": "http://localhost:%d" % server.server_port,
            "warehouse": "warehouse", "token.provider": "bear", "token": "test-token",
        })
        requests.clear()
        yield native, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_rest_branch_operations_use_core_protocol(catalog):
    native, requests = catalog
    identifier = ("db.name", "table.name$branch_dev")
    native.create_branch(identifier, "empty")
    native.create_branch(identifier, "tagged", from_tag="tag 一")
    assert native.list_branches(identifier) == ["dev", "release"]
    native.fast_forward(identifier, "tagged")
    native.drop_branch(identifier, "empty")
    base = "/v1/test/databases/db.name/tables/table.name%24branch_dev/branches"
    assert requests == [
        ("POST", base, {"branch": "empty", "fromTag": None}),
        ("POST", base, {"branch": "tagged", "fromTag": "tag 一"}),
        ("GET", base, None),
        ("POST", base + "/tagged/forward", {}),
        ("DELETE", base + "/empty", None),
    ]


def test_string_identifier_and_branch_path_encoding(catalog):
    native, requests = catalog
    native.drop_branch("db.table", "name /?#一")
    assert requests == [(
        "DELETE", "/v1/test/databases/db/tables/table/branches/name+%2F%3F%23%E4%B8%80", None,
    )]


@pytest.mark.parametrize("operation", ["drop_branch", "fast_forward"])
@pytest.mark.parametrize("name", ["", " ", ".", ".."])
def test_branch_paths_do_not_mutate_parent_resources(catalog, operation, name):
    native, requests = catalog
    with pytest.raises(ValueError):
        getattr(native, operation)("db.table", name)
    assert requests == []


@pytest.mark.parametrize("operation", ["create_branch", "drop_branch", "fast_forward", "list_branches"])
@pytest.mark.parametrize("identifier", ["db", ".table", "db.", ("", "t"), ("db", ""), "db.table$snapshots"])
def test_invalid_identifiers_fail_before_sending_requests(catalog, operation, identifier):
    native, requests = catalog
    args = () if operation == "list_branches" else ("dev",)
    with pytest.raises(ValueError):
        getattr(native, operation)(identifier, *args)
    assert requests == []


@pytest.mark.parametrize("operation", ["create_branch", "drop_branch", "fast_forward", "list_branches"])
def test_rest_branch_errors_are_not_retried(catalog, operation):
    native, requests = catalog
    args = () if operation == "list_branches" else ("dev",)
    with pytest.raises(ValueError, match="does not exist"):
        getattr(native, operation)("db.missing", *args)
    assert len(requests) == 1
    with pytest.raises(ValueError, match="Forbidden"):
        getattr(native, operation)("db.forbidden", *args)
    assert len(requests) == 2


def test_duplicate_branch_is_reported(catalog):
    native, requests = catalog
    with pytest.raises(ValueError, match="Branch dev of table db.conflict already exists"):
        native.create_branch("db.conflict", "dev")
    assert len(requests) == 1


@pytest.mark.parametrize("operation", ["create_branch", "drop_branch", "fast_forward", "list_branches"])
def test_catalog_without_branch_management_reports_unsupported(tmp_path, operation):
    native = PaimonCatalog({"warehouse": str(tmp_path)})
    args = () if operation == "list_branches" else ("dev",)
    with pytest.raises(NotImplementedError, match="branch management"):
        getattr(native, operation)("db.table", *args)
