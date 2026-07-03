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

import pickle
import tempfile

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _make_table_with_data(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.rdb")
    ctx.sql("CREATE TABLE paimon.rdb.t (id INT, name STRING)")
    ctx.sql("INSERT INTO paimon.rdb.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    catalog = PaimonCatalog({"warehouse": warehouse})
    return catalog.get_table("rdb.t")


def test_read_builder_chain_exists():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        builder = table.new_read_builder()
        scan = builder.with_projection(["id"]).with_limit(2).new_scan()
        # plan() returns a Plan; deeper assertions are in later tasks.
        plan = scan.plan()
        assert plan is not None


def test_new_read_builder_plan():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan.splits()) >= 1


def test_with_projection():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().with_projection(["id"]).new_scan().plan()
        assert plan is not None


def test_with_limit():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        # limit is a planning hint; assert only that planning succeeds.
        plan = table.new_read_builder().with_limit(1).new_scan().plan()
        assert plan is not None


def test_plan_len():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan) == len(plan.splits())


def test_plan_without_filter_succeeds():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().new_scan().plan()
        assert len(plan.splits()) >= 1


def test_split_pickle_roundtrip():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        splits = table.new_read_builder().new_scan().plan().splits()
        assert len(splits) >= 1
        split = splits[0]
        restored = pickle.loads(pickle.dumps(split))
        assert restored.row_count() == split.row_count()


def _make_partitioned_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.pdb")
    ctx.sql("CREATE TABLE paimon.pdb.pt (dt STRING, id INT) PARTITIONED BY (dt)")
    ctx.sql("INSERT INTO paimon.pdb.pt VALUES ('p1', 1), ('p1', 2), ('p2', 3), ('p3', 4)")
    catalog = PaimonCatalog({"warehouse": warehouse})
    return catalog.get_table("pdb.pt")


def test_filter_equal_converts_and_plans():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        plan = table.new_read_builder().with_filter(
            {"method": "equal", "field": "id", "literals": [1]}).new_scan().plan()
        assert plan is not None


def test_filter_prunes_partition_splits():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_partitioned_table(warehouse)
        unfiltered = len(table.new_read_builder().new_scan().plan().splits())
        filtered = len(table.new_read_builder().with_filter(
            {"method": "equal", "field": "dt", "literals": ["p1"]}).new_scan().plan().splits())
        assert filtered < unfiltered


def test_filter_and_or_compound():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        pred = {"method": "and", "children": [
            {"method": "greaterOrEqual", "field": "id", "literals": [1]},
            {"method": "lessThan", "field": "id", "literals": [99]},
        ]}
        assert table.new_read_builder().with_filter(pred).new_scan().plan() is not None


def test_filter_in_notin():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        assert b.with_filter({"method": "in", "field": "id", "literals": [1, 2]}).new_scan().plan() is not None
        # Fresh builder for notIn (with_filter overwrites; avoid relying on overwrite here).
        b2 = table.new_read_builder()
        assert b2.with_filter({"method": "notIn", "field": "id", "literals": [1, 2]}).new_scan().plan() is not None


def test_filter_isnull_isnotnull():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        assert b.with_filter({"method": "isNotNull", "field": "name", "literals": []}).new_scan().plan() is not None
        b2 = table.new_read_builder()
        assert b2.with_filter({"method": "isNull", "field": "name", "literals": []}).new_scan().plan() is not None


def test_filter_null_check_with_literals_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        with pytest.raises(ValueError):
            b.with_filter({"method": "isNull", "field": "name", "literals": [1]})
        with pytest.raises(ValueError):
            b.with_filter({"method": "isNotNull", "field": "name", "literals": [1]})
        # Valid empty/missing cases still succeed.
        assert b.with_filter(
            {"method": "isNull", "field": "name", "literals": []}).new_scan().plan() is not None
        assert b.with_filter(
            {"method": "isNull", "field": "name"}).new_scan().plan() is not None


def test_filter_bool_literal_converts():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.bdb")
        ctx.sql("CREATE TABLE paimon.bdb.bt (id INT, flag BOOLEAN)")
        ctx.sql("INSERT INTO paimon.bdb.bt VALUES (1, true), (2, false)")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("bdb.bt")
        plan = table.new_read_builder().with_filter(
            {"method": "equal", "field": "flag", "literals": [True]}).new_scan().plan()
        assert plan is not None


@pytest.mark.parametrize("method", ["like", "startsWith", "not"])
def test_filter_unsupported_operator_raises(method):
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        with pytest.raises(NotImplementedError):
            table.new_read_builder().with_filter(
                {"method": method, "field": "name", "literals": ["x"]})


def test_filter_unsupported_operator_precedes_shape_errors():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        # 'not' with no field -> NotImplementedError, not ValueError about missing field
        with pytest.raises(NotImplementedError):
            b.with_filter({"method": "not", "children": []})
        # 'like' with unknown field -> NotImplementedError, not ValueError about unknown field
        with pytest.raises(NotImplementedError):
            b.with_filter({"method": "like", "field": "nope", "literals": ["x"]})


def test_filter_unsupported_type_raises():
    # Build a table with a timestamp column, filter on it -> NotImplementedError
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.tdb")
        ctx.sql("CREATE TABLE paimon.tdb.tt (id INT, created_at TIMESTAMP)")
        ctx.sql("INSERT INTO paimon.tdb.tt VALUES (1, TIMESTAMP '2024-01-01 00:00:00')")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.tt")
        with pytest.raises(NotImplementedError):
            table.new_read_builder().with_filter(
                {"method": "equal", "field": "created_at", "literals": [0]})


def test_filter_unknown_field_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        with pytest.raises(ValueError):
            table.new_read_builder().with_filter(
                {"method": "equal", "field": "nope", "literals": [1]})


def test_filter_type_mismatch_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        with pytest.raises(ValueError):
            b.with_filter({"method": "equal", "field": "id", "literals": [True]})
        with pytest.raises(ValueError):
            b.with_filter({"method": "equal", "field": "id", "literals": ["x"]})
        with pytest.raises(ValueError):
            b.with_filter({"method": "equal", "field": "id", "literals": [None]})


def test_filter_out_of_range_raises():
    # needs a TinyInt column
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.ndb")
        ctx.sql("CREATE TABLE paimon.ndb.nt (id INT, small TINYINT)")
        ctx.sql("INSERT INTO paimon.ndb.nt VALUES (1, 5)")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("ndb.nt")
        with pytest.raises(ValueError):
            table.new_read_builder().with_filter(
                {"method": "equal", "field": "small", "literals": [9999]})


def test_filter_wrong_literal_count_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        with pytest.raises(ValueError):
            b.with_filter({"method": "equal", "field": "id", "literals": [1, 2]})
        with pytest.raises(ValueError):
            b.with_filter({"method": "in", "field": "id", "literals": []})


def test_filter_compound_with_unsupported_child_fails():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        pred = {"method": "and", "children": [
            {"method": "equal", "field": "id", "literals": [1]},
            {"method": "like", "field": "name", "literals": ["a%"]},
        ]}
        with pytest.raises(NotImplementedError):
            table.new_read_builder().with_filter(pred)


def test_filter_empty_children_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        with pytest.raises(ValueError):
            table.new_read_builder().with_filter({"method": "and", "children": []})


def test_filter_overwrite():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_partitioned_table(warehouse)
        only_p2 = len(table.new_read_builder().with_filter(
            {"method": "equal", "field": "dt", "literals": ["p2"]}).new_scan().plan().splits())
        overwritten = len(table.new_read_builder()
            .with_filter({"method": "equal", "field": "dt", "literals": ["p1"]})
            .with_filter({"method": "equal", "field": "dt", "literals": ["p2"]})
            .new_scan().plan().splits())
        assert overwritten == only_p2


def test_read_returns_expected_rows():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        splits = b.new_scan().plan().splits()
        batches = b.new_read().read(splits)
        t = pa.Table.from_batches(batches)
        assert t.num_rows == 3
        assert t.sort_by("id").to_pydict() == {"id": [1, 2, 3], "name": ["a", "b", "c"]}


def test_read_empty_splits():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        assert table.new_read_builder().new_read().read([]) == []


def test_read_empty_splits_still_validates_projection():
    # An invalid projection must fail regardless of split count; the empty-splits
    # fast path should not bypass config validation.
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        read = table.new_read_builder().with_projection(["does_not_exist"]).new_read()
        with pytest.raises(Exception):
            read.read([])


def test_read_non_split_raises_typeerror():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        with pytest.raises(TypeError):
            table.new_read_builder().new_read().read([object()])


def test_read_pickled_split():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        splits = b.new_scan().plan().splits()
        pickled = [pickle.loads(pickle.dumps(s)) for s in splits]
        batches = b.new_read().read(pickled)
        t = pa.Table.from_batches(batches)
        assert t.num_rows == 3
        assert sorted(t.column("id").to_pylist()) == [1, 2, 3]


def test_read_projection_applied():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder().with_projection(["id"])
        splits = b.new_scan().plan().splits()
        batches = b.new_read().read(splits)
        t = pa.Table.from_batches(batches)
        assert t.schema.names == ["id"]
        assert sorted(t.column("id").to_pylist()) == [1, 2, 3]


def test_read_is_config_snapshot():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder().with_projection(["id"])
        splits = b.new_scan().plan().splits()
        read = b.new_read()
        b.with_projection(["name"])  # mutate builder AFTER new_read()
        t = pa.Table.from_batches(read.read(splits))
        assert t.schema.names == ["id"]


def test_read_with_filter_smoke():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "greaterOrEqual", "field": "id", "literals": [1]})
        splits = b.new_scan().plan().splits()
        batches = b.new_read().read(splits)
        assert isinstance(batches, list)
