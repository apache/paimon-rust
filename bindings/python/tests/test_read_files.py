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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon_rust.datafusion import SQLContext


@pytest.mark.parametrize("use_catalog", [False, True])
def test_read_files_default_registration_and_named_options(tmp_path, use_catalog):
    source = tmp_path / "users.csv"
    source.write_text("id;name\n1;Alice\n2;Bob\n")
    ctx = SQLContext()
    if use_catalog:
        ctx.register_catalog("paimon", {"warehouse": str(tmp_path), "metastore": "filesystem"}, default_database="")
    query = (
        f"SELECT id, name FROM read_files('{source}', format => 'csv', "
        "schema => 'id BIGINT, name STRING', header => true, sep => ';') "
        "WHERE id = 2"
    )
    result = pa.Table.from_batches(ctx.sql(query))
    assert result.to_pydict() == {"id": [2], "name": ["Bob"]}


def test_read_files_binary_and_streaming_text(tmp_path):
    source = tmp_path / "text.txt"
    source.write_bytes(b"one\r\ntwo\n")
    ctx = SQLContext()
    reader = ctx.sql_stream(
        f"SELECT value FROM read_files('{source}', format => 'text')"
    )
    try:
        assert pa.Table.from_batches(reader).to_pydict() == {
            "value": ["one", "two"]
        }
    finally:
        reader.close()
    result = pa.Table.from_batches(ctx.sql(
        f"SELECT length, content FROM read_files('{source}', format => 'binaryFile')"
    ))
    assert result.to_pydict() == {"length": [9], "content": [b"one\r\ntwo\n"]}


def test_read_files_rejects_http():
    ctx = SQLContext()
    with pytest.raises(ValueError, match="HTTP and HTTPS"):
        ctx.sql("SELECT * FROM read_files('http://example.com/data.csv', format => 'csv')")


def test_read_files_parquet_and_json(tmp_path):
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), parquet_path)
    json_path = tmp_path / "data.json"
    json_path.write_text('[\n{"id": 3},\n{"id": 4}\n]')
    ctx = SQLContext()
    result = pa.Table.from_batches(ctx.sql(
        f"SELECT sum(id) AS total FROM read_files('{parquet_path}')"
    ))
    assert result.to_pydict() == {"total": [3]}
    result = pa.Table.from_batches(ctx.sql(
        f"SELECT sum(id) AS total FROM read_files('{json_path}', multiLine => true)"
    ))
    assert result.to_pydict() == {"total": [7]}


def test_read_files_stream_cannot_follow_a_replaced_symlink(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    source = allowed / "data.txt"
    source.write_text("safe")
    secret = tmp_path / "secret.txt"
    secret.write_text("outside secret")
    ctx = SQLContext()
    reader = ctx.sql_stream(f"SELECT value FROM read_files('{source}', format => 'text')")
    source.unlink()
    source.symlink_to(secret)
    try:
        with pytest.raises(Exception, match="[Pp]ermitted|[Pp]ermission|outside"):
            pa.Table.from_batches(reader)
    finally:
        reader.close()
