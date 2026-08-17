# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from pypaimon_rust.datafusion import PaimonCatalog


class _RESTHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/v1/config?"):
            self._respond({"defaults": {"prefix": "test"}})
        elif self.path == "/v1/test/databases":
            self._respond({"databases": ["db"], "nextPageToken": None})
        elif self.path == "/v1/test/databases/db/tables":
            self._respond({"tables": ["table"], "nextPageToken": None})
        else:
            self._respond(
                {
                    "resourceType": "table",
                    "resourceName": "missing",
                    "message": "Not Found",
                    "code": 404,
                },
                404,
            )

    def log_message(self, format, *args):
        pass

    def _respond(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def rest_server():
    server = HTTPServer(("localhost", 0), _RESTHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield "http://localhost:%d" % server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_rest_catalog_calls_release_gil(rest_server, monkeypatch):
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1")
    monkeypatch.setenv("no_proxy", "localhost,127.0.0.1")

    catalog = PaimonCatalog(
        {
            "metastore": "rest",
            "uri": rest_server,
            "warehouse": "warehouse",
            "token.provider": "bear",
            "token": "test-token",
        }
    )
    assert catalog.list_databases() == ["db"]
    assert catalog.list_tables("db") == ["table"]
    with pytest.raises(ValueError, match="does not exist"):
        catalog.get_table("db.missing")
