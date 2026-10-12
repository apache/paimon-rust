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
import pytest

from pypaimon_rust.datafusion import RecordBatchReader, SQLContext, SQLOptions


@pytest.fixture
def ctx(tmp_path):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path / "warehouse")})
    ctx.sql("SET datafusion.execution.batch_size = 2")
    ctx.sql("SET datafusion.execution.target_partitions = 1")
    return ctx


@pytest.fixture(params=["sql_stream", "sql_stream_with_options"])
def stream_sql(ctx, request):
    if request.param == "sql_stream_with_options":
        return lambda sql: ctx.sql_stream_with_options(sql, SQLOptions())
    return ctx.sql_stream


def test_sql_stream_returns_multiple_batches(ctx, stream_sql):
    sql = "SELECT value FROM generate_series(1, 9) AS t(value)"
    reader = stream_sql(sql)

    assert isinstance(reader, RecordBatchReader)
    assert iter(reader) is reader
    first = reader.read_next_batch()
    assert isinstance(first, pa.RecordBatch)
    assert first.num_rows == 2
    batches = [first, *reader]
    assert len(batches) > 1
    assert pa.Table.from_batches(batches).equals(pa.Table.from_batches(ctx.sql(sql)))
    assert reader.read_next_batch() is None
    assert list(reader) == []
    reader.close()


def test_sql_stream_reads_paimon_table(ctx, stream_sql):
    ctx.sql("CREATE TABLE paimon.default.users (id INT, name STRING)")
    ctx.sql(
        "INSERT INTO paimon.default.users VALUES "
        "(1, 'alice'), (2, NULL), (3, 'charlie')"
    )
    sql = "SELECT id, name FROM paimon.default.users WHERE id >= 2 ORDER BY id"

    result = pa.Table.from_batches(stream_sql(sql))

    assert result.equals(pa.Table.from_batches(ctx.sql(sql)))
    assert result.to_pydict() == {"id": [2, 3], "name": [None, "charlie"]}


def test_sql_stream_preserves_pyarrow_string_compatibility(ctx, stream_sql):
    ctx.register_batch(
        "labels",
        pa.record_batch(
            [pa.array(["alice", None, "charlie"], type=pa.string_view())],
            names=["name"],
        ),
    )
    sql = "SELECT name FROM labels"

    result = pa.Table.from_batches(stream_sql(sql))

    assert result.schema.field("name").type == pa.string()
    assert result.equals(pa.Table.from_batches(ctx.sql(sql)))


def test_sql_stream_empty_result(stream_sql):
    reader = stream_sql("SELECT 1 AS value WHERE FALSE")

    assert sum(batch.num_rows for batch in reader) == 0
    assert reader.read_next_batch() is None
    assert list(reader) == []
    reader.close()


def test_sql_stream_schema_describes_an_empty_result(ctx, stream_sql):
    ctx.sql("CREATE TABLE paimon.default.users (id INT, name STRING)")
    reader = stream_sql("SELECT id, name FROM paimon.default.users WHERE id < 0")

    result = pa.Table.from_batches(reader, schema=reader.schema)

    assert result.num_rows == 0
    assert result.schema.names == ["id", "name"]
    assert result.schema.types == [pa.int32(), pa.string()]


@pytest.mark.parametrize("prefix", ["", "EXPLAIN ANALYZE "])
def test_sql_stream_finishes_insert_before_returning(ctx, stream_sql, prefix):
    ctx.sql("CREATE TABLE paimon.default.events (id INT)")

    sql = f"{prefix}INSERT INTO paimon.default.events VALUES (1), (2)"
    stream_sql(sql).close()

    result = pa.Table.from_batches(
        ctx.sql("SELECT id FROM paimon.default.events ORDER BY id")
    )
    assert result.to_pydict() == {"id": [1, 2]}


def test_sql_stream_reports_planning_errors(stream_sql):
    with pytest.raises(ValueError, match="missing_table"):
        stream_sql("SELECT * FROM missing_table")


def test_sql_stream_yields_batches_before_execution_error(stream_sql):
    reader = stream_sql(
        "SELECT 10 / (3 - value) AS value FROM generate_series(1, 4) AS t(value)"
    )

    assert next(reader).to_pydict() == {"value": [5, 10]}
    with pytest.raises(ValueError, match="(?i)(divide|division) by zero"):
        reader.read_next_batch()
    assert reader.read_next_batch() is None
    reader.close()


@pytest.mark.parametrize("read_first", [False, True])
def test_sql_stream_close_stops_iteration(stream_sql, read_first):
    reader = stream_sql(
        "SELECT value FROM generate_series(1, 1000000) AS t(value)"
    )
    if read_first:
        assert next(reader).num_rows == 2

    reader.close()

    assert reader.read_next_batch() is None
    assert list(reader) == []
    reader.close()
