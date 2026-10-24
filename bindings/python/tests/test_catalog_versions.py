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

"""REST version management exposes typed core operations without Python work."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
from urllib.parse import parse_qs, urlsplit

import pytest

from pypaimon_rust.datafusion import CatalogError, Instant, PaimonCatalog


SNAPSHOT = dict(version=3, id=7, schemaId=2, baseManifestList="base", deltaManifestList="delta",
                commitUser="writer", commitIdentifier=1, commitKind="APPEND", timeMillis=123,
                uuid="snapshot-uuid", totalRecordCount=8, properties={"source": "test"},
                baseManifestListSize=321, deltaManifestListSize=0)


@pytest.fixture
def transport():
    class Handler(BaseHTTPRequestHandler):
        def operation(self):
            size = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(size)) if size else None
            self.server.requests.append((self.command, self.path, body))
            assert self.headers["Authorization"] == "Bearer token"
            data = self.server.body
            if data is None and self.server.status == 200:
                path = urlsplit(self.path).path
                if path.endswith("/snapshot"):
                    data = {"snapshot": dict(snapshot=SNAPSHOT, recordCount=0, fileCount=2,
                                             fileSizeInBytes=123, lastFileCreationTime=456)}
                elif path.endswith("/tags"):
                    data = {"tags": ["z", "a", "z"], "nextPageToken": "next +/%一"}
                elif "/tags/" in path:
                    data = dict(tagName="release", snapshot=SNAPSHOT, tagCreateTime=123,
                                tagTimeRetained="2 d")
            output = data.encode() if isinstance(data, str) else json.dumps(data).encode()
            self.send_response(self.server.status)
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)

        do_GET = do_POST = do_DELETE = operation

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    server.requests, server.status, server.body = [], 200, None
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server, PaimonCatalog({
            "metastore": "rest", "uri": "http://127.0.0.1:%d" % server.server_port,
            "warehouse": "w", "prefix": "tenant", "token.provider": "bear", "token": "token",
        }, config_required=False)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_typed_metadata_retains_snapshot_and_catalog_statistics(transport):
    server, native = transport
    snapshot = native.load_snapshot(("db.name", "table$branch_dev /一"))
    assert snapshot.snapshot().id() == 7
    assert snapshot.snapshot().uuid() == "snapshot-uuid"
    assert snapshot.snapshot().properties() == {"source": "test"}
    assert (snapshot.record_count(), snapshot.file_count(), snapshot.file_size_in_bytes(),
            snapshot.last_file_creation_time()) == (0, 2, 123, 456)
    assert json.loads(snapshot.to_json())["snapshot"]["baseManifestList"] == "base"
    assert json.loads(snapshot.to_json())["snapshot"]["baseManifestListSize"] == 321
    assert json.loads(snapshot.to_json())["snapshot"]["deltaManifestListSize"] == 0
    tag = native.get_tag("db.table", "release")
    assert tag.tag_name() == "release"
    assert tag.snapshot().id() == 7
    assert tag.tag_create_time() == 123
    assert tag.tag_time_retained() == "2 d"
    assert json.loads(tag.to_json())["snapshot"]["properties"] == {"source": "test"}
    assert json.loads(tag.to_json())["snapshot"]["baseManifestListSize"] == 321
    assert json.loads(tag.to_json())["snapshot"]["deltaManifestListSize"] == 0
    assert len(server.requests) == 2


def test_tag_page_uses_tuple_and_preserves_opaque_parameters(transport):
    server, native = transport
    page = native.list_tags_paged("db.table", 3, "next +/%一", "release +/%一")
    assert page == (["z", "a", "z"], "next +/%一")
    query = parse_qs(urlsplit(server.requests[0][1]).query)
    assert query == {"maxResults": ["3"], "pageToken": ["next +/%一"], "tagNamePrefix": ["release +/%一"]}


@pytest.mark.parametrize("instant,payload", [
    (Instant.snapshot(7), {"type": "snapshot", "snapshotId": 7}),
    (Instant.tag("release /一"), {"type": "tag", "tagName": "release /一"}),
])
@pytest.mark.parametrize("guard", [False, True])
def test_rollback_instants_and_guards_are_forwarded(transport, instant, payload, guard):
    server, native = transport
    server.body = "OK"
    kwargs = dict(from_snapshot=9, table_uuid="table-incarnation") if guard else {}
    native.rollback_to(("db.name", "table$branch_dev /一"), instant, **kwargs)
    expected = {"instant": payload}
    if guard:
        expected.update(fromSnapshot=9, tableId="table-incarnation")
    assert len(server.requests) == 1
    method, path, body = server.requests[0]
    assert method == "POST"
    assert path.endswith("/tables/table%24branch_dev+%2F%E4%B8%80/rollback")
    assert body == expected


@pytest.mark.parametrize("operation", ["create_tag", "delete_tag", "rollback_to"])
@pytest.mark.parametrize("status", [400, 403, 409, 429, 500, 501, 503])
def test_failed_mutations_are_not_replayed(transport, operation, status):
    server, native = transport
    server.status, server.body = status, {"message": "rejected"}
    args = (Instant.snapshot(7),) if operation == "rollback_to" else ("release",)
    with pytest.raises(CatalogError) as caught:
        getattr(native, operation)("db.table", *args)
    assert caught.value.code == status
    assert len(server.requests) == 1


@pytest.mark.parametrize("operation,resource,name", [
    ("create_tag", "SNAPSHOT", "7"), ("create_tag", "TAG", "release"),
    ("get_tag", "TAG", "release"), ("delete_tag", "TAG", "release"),
    ("rollback_to", "SNAPSHOT", "7"), ("rollback_to", "TAG", "release"),
    ("rollback_to", "TABLE", "db.table"), ("list_tags_paged", "TABLE", "db.table"),
])
def test_structured_version_errors_preserve_missing_resource(transport, operation, resource, name):
    server, native = transport
    server.status = 409 if operation == "create_tag" and resource == "TAG" else 404
    server.body = dict(resourceType=resource, resourceName=name, message="missing")
    if operation == "rollback_to":
        args = (Instant.snapshot(7),)
    elif operation == "list_tags_paged":
        args = ()
    else:
        args = ("release",)
        if operation == "create_tag":
            args += (7,)
    with pytest.raises(CatalogError) as caught:
        getattr(native, operation)("db.table", *args)
    assert (caught.value.code, caught.value.resource_type, caught.value.resource_name) == (
        server.status, resource, name)
    assert len(server.requests) == 1


def test_native_tag_ignore_existing_is_the_core_catalog_contract(transport):
    server, native = transport
    server.status, server.body = 409, {"resourceType": "TAG", "resourceName": "release"}
    native.create_tag("db.table", "release", ignore_if_exists=True)
    assert len(server.requests) == 1
