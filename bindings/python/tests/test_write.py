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

import tempfile

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext

# The table created by _make_empty_table is (id INT, name STRING). Paimon INT maps
# to Arrow int32, so batches must use int32 for id — pyarrow infers Python ints as
# int64, which write_arrow now (correctly, matching pypaimon) rejects as a type
# mismatch. Build batches against this explicit schema to match the table.
_TABLE_SCHEMA = pa.schema([("id", pa.int32()), ("name", pa.string())])


def _batch(ids, names):
    return pa.record_batch([ids, names], schema=_TABLE_SCHEMA)


def _make_empty_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.wdb")
    ctx.sql("CREATE TABLE paimon.wdb.t (id INT, name STRING)")
    return ctx


def _get_table(warehouse):
    return PaimonCatalog({"warehouse": warehouse}).get_table("wdb.t")


def test_write_commit_read_roundtrip():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = _make_empty_table(warehouse)
        table = _get_table(warehouse)
        batch = _batch([1, 2, 3], ["a", "b", "c"])
        wb = table.new_write_builder()
        write = wb.new_write()
        write.write_arrow(batch)
        messages = write.prepare_commit()
        assert len(messages) >= 1                # cover API shape in the first test
        wb.new_commit().commit(messages)   # same wb → shared commit_user
        result = pa.Table.from_batches(
            ctx.sql("SELECT id, name FROM paimon.wdb.t")
        ).sort_by("id").to_pydict()
        assert result == {"id": [1, 2, 3], "name": ["a", "b", "c"]}


def test_write_multiple_batches():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = _make_empty_table(warehouse)
        table = _get_table(warehouse)
        wb = table.new_write_builder()
        write = wb.new_write()
        write.write_arrow(_batch([1], ["a"]))
        write.write_arrow(_batch([2], ["b"]))
        messages = write.prepare_commit()
        wb.new_commit().commit(messages)
        result = pa.Table.from_batches(
            ctx.sql("SELECT id, name FROM paimon.wdb.t")
        ).sort_by("id").to_pydict()
        assert result == {"id": [1, 2], "name": ["a", "b"]}


def test_prepare_commit_returns_messages():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_empty_table(warehouse)
        table = _get_table(warehouse)
        write = table.new_write_builder().new_write()
        write.write_arrow(_batch([1], ["a"]))
        messages = write.prepare_commit()
        assert len(messages) >= 1
        assert all(type(m).__name__ == "CommitMessage" for m in messages)


def test_commit_empty_messages_noop():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = _make_empty_table(warehouse)
        table = _get_table(warehouse)
        wb = table.new_write_builder()
        messages = wb.new_write().prepare_commit()   # no write
        assert messages == []
        wb.new_commit().commit(messages)             # no-op success
        batches = ctx.sql("SELECT COUNT(*) AS cnt FROM paimon.wdb.t")
        assert batches[0].column(0).to_pylist() == [0]


def test_write_arrow_type_mismatch_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_empty_table(warehouse)          # table (id INT, name STRING)
        table = _get_table(warehouse)
        write = table.new_write_builder().new_write()
        bad = pa.record_batch([["x", "y"], ["a", "b"]], names=["id", "name"])  # id as STRING
        with pytest.raises(ValueError):
            write.write_arrow(bad)


def test_write_arrow_binary_family_mismatch_raises():
    # A BINARY column requires Arrow `binary`; a near-equivalent `large_binary`
    # must be rejected at validation (it would otherwise fail deeper, since the
    # write path downcasts binary fields to arrow_array::BinaryArray only).
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.wdb")
        ctx.sql("CREATE TABLE paimon.wdb.bt (id INT, data BINARY)")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("wdb.bt")
        write = table.new_write_builder().new_write()
        schema = pa.schema([("id", pa.int32()), ("data", pa.large_binary())])
        bad = pa.record_batch([[1], [b"x"]], schema=schema)
        with pytest.raises(ValueError):
            write.write_arrow(bad)


def test_commit_non_message_raises_typeerror():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_empty_table(warehouse)
        table = _get_table(warehouse)
        with pytest.raises(TypeError):
            table.new_write_builder().new_commit().commit([object()])
        # A non-iterable argument also raises TypeError (not a raw PyO3 error).
        with pytest.raises(TypeError):
            table.new_write_builder().new_commit().commit(42)


def test_commit_cross_table_messages_raises():
    # Messages prepared for one table must not be committed by another table's
    # committer (would persist a snapshot referencing data files written
    # elsewhere). The wrapper stamps each message with its source table location
    # and the committer rejects mismatches.
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.wdb")
        ctx.sql("CREATE TABLE paimon.wdb.t1 (id INT, name STRING)")
        ctx.sql("CREATE TABLE paimon.wdb.t2 (id INT, name STRING)")
        catalog = PaimonCatalog({"warehouse": warehouse})
        t1 = catalog.get_table("wdb.t1")
        t2 = catalog.get_table("wdb.t2")
        batch = pa.record_batch(
            [pa.array([1], pa.int32()), pa.array(["a"], pa.string())],
            names=["id", "name"],
        )
        w1 = t1.new_write_builder().new_write()
        w1.write_arrow(batch)
        messages = w1.prepare_commit()
        with pytest.raises(ValueError):
            t2.new_write_builder().new_commit().commit(messages)


def test_commit_different_builder_same_table_raises():
    # Even for the same table, a committer from a different WriteBuilder must
    # reject the messages: each builder mints its own commit_user, and writers
    # and committers must share one (snapshot duplicate detection / postpone
    # bucket file naming depend on it).
    with tempfile.TemporaryDirectory() as warehouse:
        _make_empty_table(warehouse)
        table = _get_table(warehouse)
        write = table.new_write_builder().new_write()
        write.write_arrow(_batch([1], ["a"]))
        messages = write.prepare_commit()
        with pytest.raises(ValueError):
            table.new_write_builder().new_commit().commit(messages)
