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


def test_with_row_ranges():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.rdb")
        ctx.sql("""CREATE TABLE paimon.rdb.de (id INT, name STRING) WITH (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true')""")
        ctx.sql("""INSERT INTO paimon.rdb.de (id, name)
            VALUES (1, 'a'), (2, 'b'), (3, 'c')""")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("rdb.de")
        builder = table.new_read_builder().with_row_ranges([(0, 1)])
        plan = builder.new_scan().plan()
        batches = builder.new_read().read(plan.splits())
        assert pa.Table.from_batches(batches).column("id").to_pylist() == [1, 2]

        restored_splits = [pickle.loads(pickle.dumps(split)) for split in plan.splits()]
        restored_batches = builder.new_read().read(restored_splits)
        assert pa.Table.from_batches(restored_batches).column("id").to_pylist() == [1, 2]

        empty_builder = table.new_read_builder().with_row_ranges([])
        empty_plan = empty_builder.new_scan().plan()
        assert empty_plan.splits() == []
        assert empty_builder.new_read().read(empty_plan.splits()) == []

        with pytest.raises(ValueError, match="start 2 exceeds end 1"):
            table.new_read_builder().with_row_ranges([(2, 1)])


def test_format_table_rejects_row_ranges():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.rdb")
        ctx.sql("""CREATE TABLE paimon.rdb.ft (id INT) WITH (
            'type' = 'format-table',
            'file.format' = 'parquet')""")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("rdb.ft")

        with pytest.raises(NotImplementedError, match="not supported for format tables"):
            table.new_read_builder().with_row_ranges([]).new_scan().plan()


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


def test_filter_unsupported_operator_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        with pytest.raises(NotImplementedError):
            table.new_read_builder().with_filter(
                {"method": "not", "field": "name", "literals": ["x"]})


def test_filter_unsupported_operator_precedes_shape_errors():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        b = table.new_read_builder()
        # 'not' with no field -> NotImplementedError, not ValueError about missing field
        with pytest.raises(NotImplementedError):
            b.with_filter({"method": "not", "children": []})


def _make_string_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.sdb")
    ctx.sql("CREATE TABLE paimon.sdb.st (id INT, name STRING)")
    # Two separate INSERTs -> two files, so file stats can prune per-file.
    ctx.sql("INSERT INTO paimon.sdb.st VALUES (1, 'apple'), (2, 'apricot')")
    ctx.sql("INSERT INTO paimon.sdb.st VALUES (3, 'banana'), (4, 'cherry')")
    return PaimonCatalog({"warehouse": warehouse}).get_table("sdb.st")


def _read_ids(builder):
    splits = builder.new_scan().plan().splits()
    batches = builder.new_read().read(splits)
    if not batches:
        return []
    return sorted(pa.Table.from_batches(batches).column("id").to_pylist())


def test_filter_starts_with_prunes_and_reads():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "startsWith", "field": "name", "literals": ["ap"]})
        assert len(b.new_scan().plan().splits()) == 1
        assert _read_ids(b) == [1, 2]


def test_filter_ends_with_reads_matching_rows():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "endsWith", "field": "name", "literals": ["y"]})
        assert _read_ids(b) == [4]


def test_filter_contains_reads_matching_rows():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "contains", "field": "name", "literals": ["an"]})
        assert _read_ids(b) == [3]


def test_filter_like_prefix_reads_matching_rows():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "like", "field": "name", "literals": ["ap%"]})
        assert _read_ids(b) == [1, 2]


def test_filter_like_residual_pattern_reads_matching_rows():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        # '_' single-char wildcard is not rewritten; exercises the Like evaluator.
        b = table.new_read_builder().with_filter(
            {"method": "like", "field": "name", "literals": ["b_nana"]})
        assert _read_ids(b) == [3]


def test_filter_like_escape_literal():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder()
        # Optional second literal is the ESCAPE character; only '\\' is accepted.
        assert b.with_filter(
            {"method": "like", "field": "name",
             "literals": ["100\\%%", "\\"]}).new_scan().plan() is not None
        with pytest.raises(ValueError):
            b.with_filter(
                {"method": "like", "field": "name", "literals": ["100!%%", "!"]})
        with pytest.raises(ValueError):
            b.with_filter(
                {"method": "like", "field": "name", "literals": ["a%", "ab"]})


def test_filter_string_op_on_non_string_column_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)
        b = table.new_read_builder()
        for method in ["startsWith", "endsWith", "contains", "like"]:
            with pytest.raises(ValueError):
                b.with_filter({"method": method, "field": "id", "literals": ["a"]})


def test_filter_unsupported_type_raises():
    # Binary columns have no literal conversion -> NotImplementedError
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.tdb")
        ctx.sql("CREATE TABLE paimon.tdb.tt (id INT, payload BYTEA)")
        ctx.sql("INSERT INTO paimon.tdb.tt VALUES (1, X'00')")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.tt")
        with pytest.raises(NotImplementedError):
            table.new_read_builder().with_filter(
                {"method": "equal", "field": "payload", "literals": [0]})


def _make_temporal_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.tmp")
    ctx.sql(
        "CREATE TABLE paimon.tmp.tt (id INT, d DATE, ts TIMESTAMP, dec DECIMAL(10, 2))")
    # Two separate INSERTs -> two files, so file stats can prune per-file.
    ctx.sql(
        "INSERT INTO paimon.tmp.tt VALUES "
        "(1, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 12.34)")
    ctx.sql(
        "INSERT INTO paimon.tmp.tt VALUES "
        "(2, DATE '2024-06-15', TIMESTAMP '2024-06-15 12:30:00', 56.78)")
    return PaimonCatalog({"warehouse": warehouse}).get_table("tmp.tt")


def test_filter_date_literal_prunes_and_reads():
    import datetime

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_temporal_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "equal", "field": "d", "literals": [datetime.date(2024, 1, 1)]})
        splits = b.new_scan().plan().splits()
        assert len(splits) == 1
        t = pa.Table.from_batches(b.new_read().read(splits))
        assert t.column("id").to_pylist() == [1]


def test_filter_timestamp_literal_prunes_and_reads():
    import datetime

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_temporal_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "greaterThan", "field": "ts",
             "literals": [datetime.datetime(2024, 3, 1, 0, 0, 0)]})
        splits = b.new_scan().plan().splits()
        assert len(splits) == 1
        t = pa.Table.from_batches(b.new_read().read(splits))
        assert t.column("id").to_pylist() == [2]


def test_filter_timestamp_rejects_aware_datetime():
    import datetime

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_temporal_table(warehouse)
        aware = datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)
        with pytest.raises(ValueError):
            table.new_read_builder().with_filter(
                {"method": "greaterThan", "field": "ts", "literals": [aware]})


def test_filter_decimal_literal_prunes_and_reads():
    from decimal import Decimal

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_temporal_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "equal", "field": "dec", "literals": [Decimal("12.34")]})
        splits = b.new_scan().plan().splits()
        assert len(splits) == 1
        t = pa.Table.from_batches(b.new_read().read(splits))
        assert t.column("id").to_pylist() == [1]


def test_filter_decimal_rejects_float_and_rounding():
    from decimal import Decimal

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_temporal_table(warehouse)
        b = table.new_read_builder()
        with pytest.raises(ValueError):
            b.with_filter({"method": "equal", "field": "dec", "literals": [12.34]})
        with pytest.raises(ValueError):
            b.with_filter(
                {"method": "equal", "field": "dec", "literals": [Decimal("0.005")]})


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
            {"method": "not", "children": [
                {"method": "equal", "field": "name", "literals": ["a"]},
            ]},
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


def _make_two_snapshot_table(warehouse):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": warehouse})
    ctx.sql("CREATE SCHEMA paimon.tdb")
    ctx.sql("CREATE TABLE paimon.tdb.t (id INT, name STRING)")
    ctx.sql("INSERT INTO paimon.tdb.t VALUES (1, 'a')")            # snapshot 1
    ctx.sql("INSERT INTO paimon.tdb.t VALUES (2, 'b'), (3, 'c')")  # snapshot 2
    return ctx


def _rows(batches):
    return sum(b.num_rows for b in batches)


def test_time_travel_by_snapshot_id():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        builder = table.new_read_builder({"scan.snapshot-id": "1"})
        splits = builder.new_scan().plan().splits()
        batches = builder.new_read().read(splits)
        assert _rows(batches) == 1  # only snapshot 1's row


def test_time_travel_by_tag_name():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = _make_two_snapshot_table(warehouse)
        ctx.sql("CALL sys.create_tag(table => 'tdb.t', tag => 'v1', snapshot_id => 1)")
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        builder = table.new_read_builder({"scan.tag-name": "v1"})
        splits = builder.new_scan().plan().splits()
        assert _rows(builder.new_read().read(splits)) == 1


def test_time_travel_unresolved_snapshot_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(ValueError, match="did not resolve"):
            table.new_read_builder({"scan.snapshot-id": "999"})


def test_time_travel_unresolved_watermark_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        # The Rust commit path never writes watermarks, so no snapshot matches;
        # the binding must raise instead of silently reading latest.
        with pytest.raises(ValueError, match="did not resolve"):
            table.new_read_builder({"scan.watermark": "1"})


def test_unsupported_scan_option_raises_not_implemented():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(NotImplementedError):
            table.new_read_builder({"incremental-between": "1,2"})


def test_explicit_scan_mode_with_matching_selector_reads_snapshot():
    # Java's CoreOptions.setDefaultValues() writes scan.mode=from-snapshot next
    # to scan.snapshot-id, so configs from the Java toolchain carry both keys.
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        builder = table.new_read_builder(
            {"scan.mode": "from-snapshot", "scan.snapshot-id": "1"})
        splits = builder.new_scan().plan().splits()
        assert _rows(builder.new_read().read(splits)) == 1


def test_explicit_scan_mode_without_selector_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(ValueError, match="from-snapshot"):
            table.new_read_builder({"scan.mode": "from-snapshot"})


def test_unimplemented_scan_mode_raises_not_implemented():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(NotImplementedError):
            table.new_read_builder({"scan.mode": "incremental"})


def test_scan_option_non_string_value_raises_type_error():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(TypeError):
            table.new_read_builder({"scan.snapshot-id": 1})


def test_new_read_builder_none_options_reads_latest():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        builder = table.new_read_builder()  # no options → latest
        splits = builder.new_scan().plan().splits()
        assert _rows(builder.new_read().read(splits)) == 3


def test_time_travel_filter_uses_travelled_schema():
    # Snapshot 1 predates the 'age' column. Travelling to it and filtering on
    # 'age' must fail (not in the travelled schema), while filtering on a
    # column present in that schema ('name') must succeed. This proves
    # with_filter validates against the travelled schema, not the latest one.
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE SCHEMA paimon.edb")
        ctx.sql("CREATE TABLE paimon.edb.t (id INT, name STRING)")
        ctx.sql("INSERT INTO paimon.edb.t VALUES (1, 'a')")           # snapshot 1, schema 0
        ctx.sql("ALTER TABLE paimon.edb.t ADD COLUMN age INT")
        ctx.sql("INSERT INTO paimon.edb.t VALUES (2, 'b', 20)")       # snapshot 2, schema 1
        catalog = PaimonCatalog({"warehouse": warehouse})

        # Filtering on the post-travel-absent column must fail.
        travelled = catalog.get_table("edb.t").new_read_builder({"scan.snapshot-id": "1"})
        with pytest.raises(ValueError):
            travelled.with_filter({"method": "equal", "field": "age", "literals": [20]})

        # Filtering on a column present in the travelled schema must succeed.
        travelled_ok = catalog.get_table("edb.t").new_read_builder({"scan.snapshot-id": "1"})
        plan = travelled_ok.with_filter(
            {"method": "equal", "field": "name", "literals": ["a"]}
        ).new_scan().plan()
        assert plan is not None


def test_time_travel_conflicting_selectors_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(ValueError, match="Only one time-travel selector") as exc:
            table.new_read_builder({"scan.snapshot-id": "1", "scan.tag-name": "t"})
        # both offending keys are named
        assert "scan.snapshot-id" in str(exc.value)
        assert "scan.tag-name" in str(exc.value)


def test_time_travel_watermark_conflicting_selector_raises():
    with tempfile.TemporaryDirectory() as warehouse:
        _make_two_snapshot_table(warehouse)
        table = PaimonCatalog({"warehouse": warehouse}).get_table("tdb.t")
        with pytest.raises(ValueError, match="Only one time-travel selector") as exc:
            table.new_read_builder({"scan.watermark": "1", "scan.snapshot-id": "1"})
        assert "scan.watermark" in str(exc.value)
        assert "scan.snapshot-id" in str(exc.value)


def test_split_serialize_produces_split_v1_binary():
    import struct

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_table_with_data(warehouse)
        splits = table.new_read_builder().new_scan().plan().splits()
        assert splits
        data = splits[0].serialize()
        assert isinstance(data, (bytes, bytearray))
        # SplitSerializer frame: "SPLIT_V1" + version(1) + type-id(1=DATA_SPLIT)
        assert data[:8] == b"SPLIT_V1"
        version, type_id = struct.unpack_from(">ii", data, 8)
        assert version == 1
        assert type_id == 1
        # DataSplit v8 body follows: MAGIC + VERSION(8)
        magic, body_version = struct.unpack_from(">qi", data, 16)
        assert magic == -2394839472490812314
        assert body_version == 8
        assert splits[0].serialize() == data  # deterministic


def test_split_serialize_carries_partition_and_reads():
    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_partitioned_table(warehouse)
        b = table.new_read_builder().with_filter(
            {"method": "equal", "field": "dt", "literals": ["p1"]})
        splits = b.new_scan().plan().splits()
        assert splits
        assert b"p1" in splits[0].serialize()  # partition value encoded
        # partition filter pruned to p1
        t = pa.Table.from_batches(b.new_read().read(splits))
        assert sorted(t.column("id").to_pylist()) == [1, 2]


def test_split_serialize_encodes_deletions_and_external_path():
    # plan() can't produce these; inject them via the serde payload.
    import json

    with tempfile.TemporaryDirectory() as warehouse:
        table = _make_string_table(warehouse)  # two files
        split = table.new_read_builder().new_scan().plan().splits()[0]
        cls, (raw,) = split.__reduce__()
        payload = json.loads(bytes(raw))
        n = len(payload["data_files"])
        assert n >= 2
        payload["data_deletion_files"] = [
            {"path": "dv/idx-0", "offset": 4, "length": 20, "cardinality": 3}
        ] + [None] * (n - 1)
        payload["data_files"][0]["_EXTERNAL_PATH"] = "s3://ext/data-0.parquet"

        data = cls(json.dumps(payload).encode()).serialize()
        assert b"dv/idx-0" in data                 # deletion file path
        assert b"s3://ext/data-0.parquet" in data  # external path
