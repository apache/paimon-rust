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

import os
import tempfile

import pyarrow as pa
from datafusion import SessionContext

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext

WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")


def extract_rows(batches):
    table = pa.Table.from_batches(batches)
    return sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))


def test_query_simple_table_via_catalog_provider():
    catalog = PaimonCatalog({"warehouse": WAREHOUSE})
    ctx = SessionContext()
    ctx.register_catalog_provider("paimon", catalog)

    df = ctx.sql("SELECT id, name FROM paimon.default.simple_log_table")

    assert extract_rows(df.collect()) == [
        (1, "alice"),
        (2, "bob"),
        (3, "carol"),
    ]


def test_sql_context_ddl_dml():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql("CREATE SCHEMA paimon.test_db")
        ctx.sql(
            "CREATE TABLE paimon.test_db.users "
            "(id INT, name STRING, PRIMARY KEY (id))"
        )

        ctx.sql("INSERT INTO paimon.test_db.users VALUES (1, 'alice'), (2, 'bob')")

        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TABLE paimon.test_db.users")
        ctx.sql("DROP SCHEMA paimon.test_db")


def test_register_batch_fully_qualified():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
        ctx.register_batch("paimon.default.my_temp", batch)

        batches = ctx.sql("SELECT id, name FROM paimon.default.my_temp")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_batch_bare_name():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
        # Bare name uses current catalog and current database
        ctx.register_batch("my_temp", batch)

        batches = ctx.sql("SELECT id, name FROM paimon.default.my_temp")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_temp_table_shadows_paimon_table():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql("CREATE SCHEMA paimon.test_db")
        ctx.sql("CREATE TABLE paimon.test_db.users (id INT, name STRING)")
        ctx.sql("INSERT INTO paimon.test_db.users VALUES (1, 'real')")

        batch = pa.record_batch([[2], ["temp"]], names=["id", "name"])
        ctx.register_batch("paimon.test_db.users", batch)

        # Temp table should shadow the real Paimon table
        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(2, "temp")]

        ctx.sql("DROP TEMPORARY TABLE paimon.test_db.users")

        # After dropping, the real table is visible again
        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "real")]

        ctx.sql("DROP TABLE paimon.test_db.users")
        ctx.sql("DROP SCHEMA paimon.test_db")


def test_drop_temp_table_if_exists():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1]], names=["id"])
        ctx.register_batch("paimon.default.my_temp", batch)

        ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.default.my_temp")

        # Should be able to drop again without error
        ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.default.my_temp")


def test_multi_catalog_temp_table():
    with tempfile.TemporaryDirectory() as wh1, tempfile.TemporaryDirectory() as wh2:
        ctx = SQLContext()
        ctx.register_catalog("cat1", {"warehouse": wh1})
        ctx.register_catalog("cat2", {"warehouse": wh2})

        batch1 = pa.record_batch([[1]], names=["id"])
        batch2 = pa.record_batch([[2]], names=["id"])

        ctx.register_batch("cat1.default.t1", batch1)
        ctx.register_batch("cat2.default.t2", batch2)

        result1 = ctx.sql("SELECT id FROM cat1.default.t1")
        assert pa.Table.from_batches(result1)["id"].to_pylist() == [1]

        result2 = ctx.sql("SELECT id FROM cat2.default.t2")
        assert pa.Table.from_batches(result2)["id"].to_pylist() == [2]

        ctx.sql("DROP TEMPORARY TABLE cat1.default.t1")
        ctx.sql("DROP TEMPORARY TABLE cat2.default.t2")


def test_register_batch_invalid_catalog():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1]], names=["id"])
        try:
            ctx.register_batch("unknown_catalog.default.my_temp", batch)
            assert False, "Expected an error for unknown catalog"
        except Exception as e:
            assert "unknown_catalog" in str(e).lower() or "not a paimon" in str(e).lower() or "unknown" in str(e).lower()
