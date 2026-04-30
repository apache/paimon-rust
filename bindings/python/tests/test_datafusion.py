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
