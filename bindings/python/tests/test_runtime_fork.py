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

import multiprocessing

import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _plan_rows(catalog):
    assert "fork_db" in catalog.list_databases()
    builder = catalog.get_table("fork_db.t").new_read_builder()
    plan = builder.new_scan().plan()
    assert plan.snapshot_id() == 1
    assert sum(split.row_count() for split in plan.splits()) == 3
    return [split.serialize() for split in plan.splits()]


def _plan_in_child(warehouse, inherited_catalog, result):
    catalog = inherited_catalog or PaimonCatalog({"warehouse": warehouse})
    # The second call also exercises reuse of the child's runtime.
    expected = _plan_rows(catalog)
    assert _plan_rows(catalog) == expected
    result.send(expected)
    result.close()


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="requires fork")
@pytest.mark.parametrize("reuse_catalog", [False, True])
def test_fork_after_parent_planning(tmp_path, reuse_catalog):
    warehouse = str(tmp_path)
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.fork_db")
    ctx.sql("CREATE TABLE paimon.fork_db.t (id INT)")
    ctx.sql("INSERT INTO paimon.fork_db.t VALUES (1), (2), (3)")
    catalog = PaimonCatalog({"warehouse": warehouse})
    expected = _plan_rows(catalog)

    context = multiprocessing.get_context("fork")
    received, sent = context.Pipe(duplex=False)
    child = context.Process(
        target=_plan_in_child,
        args=(warehouse, catalog if reuse_catalog else None, sent),
    )
    child.start()
    sent.close()
    try:
        assert received.poll(20), "child planning blocked after inheriting the parent runtime"
        assert received.recv() == expected
        child.join(5)
        assert child.exitcode == 0
    finally:
        received.close()
        if child.is_alive():
            child.terminate()
            child.join(5)
        if child.is_alive():
            child.kill()
            child.join(5)
        child.close()

    # Replacing the child's runtime must not disrupt its parent.
    assert _plan_rows(catalog) == expected
