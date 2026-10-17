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
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote_plus

import pytest

from pypaimon_rust.datafusion import CatalogError, PaimonCatalog


@pytest.fixture
def rest_server(monkeypatch):
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
            elif "nocode" in parts:
                payload, status = {"message": "denied"}, 403
            elif any(part.startswith("status_") for part in parts):
                status = int(next(part for part in parts if part.startswith("status_")).split("_")[1])
                payload = {"message": "unmapped status"}
            elif "invalidjson" in parts:
                payload, status = "not a branches response", 200
            elif body and body.get("fromTag") == "missing":
                payload, status = {"resourceType": "TAG", "message": "tag missing", "code": 404}, 404
            elif self.command == "GET":
                payload, status = {"branches": ["dev", "release"]}, 200
            else:
                payload, status = None, 204
            output = json.dumps(payload).encode() if status != 204 else b""
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(output)))
            if "cookies" in parts:
                if len(requests) > 1:
                    assert self.headers["Cookie"] == "sid=accepted"
                self.send_header("Set-Cookie", "sid=accepted; Path=/v1/")
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
        yield {
            "metastore": "rest", "uri": "http://localhost:%d" % server.server_port,
            "warehouse": "warehouse", "token.provider": "bear", "token": "test-token",
        }, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def catalog(rest_server):
    options, requests = rest_server
    native = PaimonCatalog(options)
    requests.clear()
    return native, requests


def test_resolved_rest_configuration_is_not_fetched_again(rest_server):
    options, requests = rest_server
    native = PaimonCatalog(dict(options, prefix="resolved-prefix"), config_required=False)
    assert native.list_branches("db.t") == ["dev", "release"]
    assert requests == [("GET", "/v1/resolved-prefix/databases/db/tables/t/branches", None)]


@pytest.mark.parametrize("capture", [False, True])
def test_response_cookie_capture_is_opt_in_but_jar_always_persists(rest_server, capture):
    options, requests = rest_server
    native = PaimonCatalog(options, config_required=False, _capture_response_cookies=capture)
    for _ in range(2):
        assert native.list_branches("db.cookies") == ["dev", "release"]
        updates = native._take_response_cookies()
        if capture:
            assert len(updates) == 1
            assert updates[0][:2] == (
                options["uri"] + "/v1/databases/db/tables/cookies/branches",
                ["sid=accepted; Path=/v1/"],
            )
            assert abs(time.time() - updates[0][2]) < 2
        else:
            assert updates == []
        assert native._take_response_cookies() == []
    assert len(requests) == 2


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


@pytest.mark.parametrize("operation,identifier,args,code,resource_type,resource_name,message", [
    ("create_branch", "db.conflict", ("dev",), 409, "BRANCH", "dev", "already exists"),
    ("create_branch", "db.missing", ("dev",), 404, "TABLE", "db.missing", "does not exist"),
    ("create_branch", "db.table", ("dev", "missing"), 404, "TAG", "missing", "does not exist"),
    ("drop_branch", "db.missing", ("dev",), 404, "BRANCH", "dev", "does not exist"),
    ("fast_forward", "db.missing", ("dev",), 404, "BRANCH", "dev", "does not exist"),
    ("list_branches", "db.missing", (), 404, "TABLE", "db.missing", "does not exist"),
    ("list_branches", "db.forbidden", (), 403, None, None, "denied"),
    ("list_branches", "db.nocode", (), 403, None, None, "denied"),
    ("create_branch", "db.status_429", ("dev",), 429, None, None, "unmapped status"),
    ("list_branches", "db.status_502", (), 502, None, None, "unmapped status"),
    ("list_branches", "db.status_504", (), 504, None, None, "unmapped status"),
    ("list_branches", "db.invalidjson", (), 0, None, None, "failed to parse json"),
])
def test_catalog_errors_preserve_structured_details(
        catalog, operation, identifier, args, code, resource_type, resource_name, message):
    native, requests = catalog
    with pytest.raises(CatalogError) as raised:
        getattr(native, operation)(identifier, *args)
    error = raised.value
    assert error.code == code
    assert error.resource_type == resource_type
    assert error.resource_name == resource_name
    assert message in error.message
    assert isinstance(error, ValueError)
    assert len(requests) == 1


def test_transport_failure_has_structured_catalog_error():
    import socket

    with socket.socket() as listener:
        listener.bind(("localhost", 0))
        native = PaimonCatalog({
            "metastore": "rest", "uri": "http://localhost:%d" % listener.getsockname()[1],
            "warehouse": "warehouse", "token.provider": "bear", "token": "test-token",
        }, config_required=False)
        with pytest.raises(CatalogError) as raised:
            native.drop_branch("db.t", "dev")
    assert raised.value.code == 0
    assert raised.value.resource_type is None
    assert raised.value.resource_name is None
    assert "http delete failed" in raised.value.message


@pytest.mark.parametrize("operation", ["create_branch", "drop_branch", "fast_forward", "list_branches"])
def test_catalog_without_branch_management_reports_unsupported(tmp_path, operation):
    native = PaimonCatalog({"warehouse": str(tmp_path)})
    args = () if operation == "list_branches" else ("dev",)
    with pytest.raises(NotImplementedError, match="branch management"):
        getattr(native, operation)("db.table", *args)
