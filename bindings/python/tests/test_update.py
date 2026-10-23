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
import pypaimon_rust.datafusion as datafusion

from pypaimon_rust.datafusion import (
    MergeAssignment, PaimonCatalog, SQLContext, WhenMatched, WhenNotMatched,
)


@pytest.mark.parametrize('stream', [False, True])
def test_arrow_row_id_deletes_preserve_captured_bucket_vectors(tmp_path, stream):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.arrow_deletes')
    context.sql("""CREATE TABLE paimon.arrow_deletes.t (id INT) WITH (
        'bucket' = '-1', 'row-tracking.enabled' = 'true',
        'data-evolution.enabled' = 'true', 'deletion-vectors.enabled' = 'true')""")
    for values in ['(10), (11)', '(20), (21)']:
        context.sql('INSERT INTO paimon.arrow_deletes.t (id) VALUES ' + values)
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('arrow_deletes.t')
    builder = (table.new_stream_write_builder().with_commit_user('arrow-deletes')
               if stream else table.new_batch_write_builder())

    def delete(update, row_ids, identifier):
        return (update.delete_by_row_id(row_ids, identifier) if stream
                else update.delete_by_row_id(row_ids))

    def commit(messages, identifier):
        committer = builder.new_commit()
        committer.commit(identifier, messages) if stream else committer.commit(messages)

    def rows():
        return pa.Table.from_batches(context.sql(
            'SELECT id FROM paimon.arrow_deletes.t ORDER BY id')).column('id').to_pylist()

    # Keep the legacy sequence input and seed a DV for a different anchor.
    commit(delete(builder.new_update(), [0], 40), 40)
    plan = table.new_read_builder().new_scan().plan_all()
    groups = datafusion.RowIdPlan(plan).groups()
    selected = datafusion.Plan(plan.snapshot_id(), [split for first, _, split in groups if first >= 2])
    update = builder.new_update().with_plan(selected)
    context.sql('INSERT INTO paimon.arrow_deletes.t (id) VALUES (30)')

    ids = pa.chunked_array([
        pa.array([999, 2, 2], type=pa.uint64()).slice(1),
        pa.array([3, 999], type=pa.uint64()).slice(0, 1),
    ])
    messages = delete(update, ids, 41)
    stale = delete(update, pa.array([3], type=pa.int16()), 42)
    assert rows() == [11, 20, 21, 30]
    commit(messages, 41)
    assert rows() == [11, 30]
    # A second preparation from the captured snapshot cannot replace its old
    # index again after the first publication.
    snapshot = table.latest_snapshot().id()
    with pytest.raises(ValueError, match='missing deletion vector index'):
        commit(stale, 42)
    assert table.latest_snapshot().id() == snapshot
    assert rows() == [11, 30]


@pytest.mark.parametrize('stream', [False, True])
def test_arrow_row_id_delete_input_validation(tmp_path, stream):
    from pathlib import Path

    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.delete_inputs')
    context.sql("""CREATE TABLE paimon.delete_inputs.t (id INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    context.sql('INSERT INTO paimon.delete_inputs.t (id) VALUES (10)')
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('delete_inputs.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_plan(table.new_read_builder().new_scan().plan_all())
    options = {'commit_identifier': 7} if stream else {}

    for empty in [[], pa.array([], type=pa.int8()), pa.chunked_array([], type=pa.uint64())]:
        assert update.delete_by_row_id(empty, **options) == []
    for ids in [
        (0,),
        pa.array([0], type=pa.int8()),
        pa.array([0], type=pa.uint32()),
        pa.array([None, 0, None], type=pa.int64()).slice(1, 1),
        pa.chunked_array([pa.array([], type=pa.uint64()), pa.array([0], type=pa.uint64())]),
    ]:
        assert update.delete_by_row_id(ids, **options)

    def index_files():
        return {path for path in Path(table.location()).rglob('index-*') if path.is_file()}

    prepared_indexes = index_files()
    assert prepared_indexes
    for ids, error in [
        (pa.array([None], type=pa.int64()), ValueError),
        (pa.chunked_array([[0], [None]], type=pa.int64()), ValueError),
        (pa.array([1.5], type=pa.float64()), TypeError),
        (pa.chunked_array([], type=pa.float64()), TypeError),
        (pa.array([], type=pa.bool_()), TypeError),
        (pa.array([], type=pa.string()), TypeError),
        (pa.array([2 ** 63], type=pa.uint64()), ValueError),
        (pa.chunked_array([[0], [2 ** 63]], type=pa.uint64()), ValueError),
        (pa.array([-1], type=pa.int64()), ValueError),
        ([1.5], TypeError),
        ([None], TypeError),
        ([2 ** 63], OverflowError),
    ]:
        with pytest.raises(error):
            update.delete_by_row_id(ids, **options)
    assert index_files() == prepared_indexes
    assert table.latest_snapshot().id() == 1


@pytest.mark.parametrize('stream', [False, True])
def test_sequence_row_id_deletes_do_not_require_pyarrow(tmp_path, stream, monkeypatch):
    import builtins

    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.sequence_deletes')
    context.sql("""CREATE TABLE paimon.sequence_deletes.t (id INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    context.sql('INSERT INTO paimon.sequence_deletes.t (id) VALUES (10), (20), (30)')

    original_import = builtins.__import__

    def without_pyarrow(name, *args, **kwargs):
        if name == 'pyarrow' or name.startswith('pyarrow.'):
            raise ModuleNotFoundError("No module named 'pyarrow'", name='pyarrow')
        return original_import(name, *args, **kwargs)

    # PyArrow is optional for the SDK. Plain sequence deletes and commits
    # must remain usable against existing tables without that dependency.
    with monkeypatch.context() as patched:
        patched.setattr(builtins, '__import__', without_pyarrow)
        table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('sequence_deletes.t')
        builder = (table.new_stream_write_builder().with_commit_user('sequence-deletes')
                   if stream else table.new_batch_write_builder())
        for identifier, row_ids in enumerate([[], [0], (1,)]):
            update = builder.new_update()
            messages = (update.delete_by_row_id(row_ids, identifier) if stream
                        else update.delete_by_row_id(row_ids))
            if not row_ids:
                assert messages == []
                continue
            assert messages
            commit = builder.new_commit()
            commit.commit(identifier, messages) if stream else commit.commit(messages)

    assert pa.Table.from_batches(context.sql(
        'SELECT id FROM paimon.sequence_deletes.t')).column('id').to_pylist() == [30]


def test_upsert_batches_preserve_missing_fields_nulls_and_stream_updates(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.shapes')
    context.sql("""CREATE TABLE paimon.shapes.t (id INT, name STRING, score INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    context.sql("INSERT INTO paimon.shapes.t (id, name, score) VALUES (1, 'a', 10), (1, 'b', 11), (2, 'c', 20)")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('shapes.t')

    def batch(values):
        schema = pa.schema([(name, pa.string() if name == 'name' else pa.int32()) for name in values])
        return pa.RecordBatch.from_pydict(values, schema=schema)

    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['name'])
    messages = update.upsert_by_arrow_with_key([
        batch({'id': [1], 'score': [999]}),
        batch({'id': [3], 'score': [30]}),
        batch({'name': [None], 'id': [1]}),
        batch({'id': [2], 'name': ['new'], 'score': [999]}),
    ], ['id'])
    builder.new_commit().commit(messages)
    assert pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.shapes.t ORDER BY id, score')).to_pylist() == [
        {'id': 1, 'name': None, 'score': 10},
        {'id': 1, 'name': None, 'score': 11},
        {'id': 2, 'name': 'new', 'score': 20},
        {'id': 3, 'name': None, 'score': 30},
    ]
    stream = table.new_stream_write_builder().with_commit_user('shape-stream')
    messages = stream.new_update().with_update_type(['score']).upsert_by_arrow_with_key(
        [batch({'id': [1], 'score': [100]}), batch({'id': [3], 'score': [300], 'name': ['ignored']})],
        ['id'], 101)
    stream.new_commit().commit(101, messages)
    assert pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.shapes.t ORDER BY id, score')).to_pylist() == [
        {'id': 1, 'name': None, 'score': 100},
        {'id': 1, 'name': None, 'score': 100},
        {'id': 2, 'name': 'new', 'score': 20},
        {'id': 3, 'name': None, 'score': 300},
    ]


def test_table_upsert_updates_duplicate_targets_and_appends(tmp_path):
    assert not hasattr(datafusion, '_match_upsert_keys')
    assert not hasattr(datafusion, 'UpsertKeyMatcher')
    assert not hasattr(datafusion, 'BatchTableDelete')
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.table_upsert')
    context.sql("""CREATE TABLE paimon.table_upsert.t (
        id INT, name STRING, score INT) WITH (
        'row-tracking.enabled' = 'true',
        'data-evolution.enabled' = 'true')""")
    context.sql("""INSERT INTO paimon.table_upsert.t (id, name, score)
        VALUES (1, 'a', 10), (1, 'b', 11), (2, 'c', 20)""")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('table_upsert.t')
    builder = table.new_batch_write_builder()
    first = pa.record_batch([
        pa.array([1], type=pa.int32()),
        pa.array(['x']),
        pa.array([100], type=pa.int32()),
    ], names=['id', 'name', 'score'])
    second = pa.record_batch([
        pa.array([1, 3], type=pa.int32()),
        pa.array(['y', 'd']),
        pa.array([101, 30], type=pa.int32()),
    ], names=['id', 'name', 'score'])
    update = builder.new_update().with_update_type(update_cols=['name', 'score'])
    assert not hasattr(update, 'add_matched_batch')
    assert not hasattr(update, 'prepare_commit')
    assert not hasattr(update, 'close')
    messages = update.upsert_by_arrow_with_key(
        table=pa.Table.from_batches([first, second]), upsert_keys=['id'])
    assert messages and all(message.serialize() for message in messages)
    builder.new_commit().commit(messages)

    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.table_upsert.t'))
    actual = actual.sort_by([('id', 'ascending'), ('name', 'ascending')]).to_pydict()
    assert actual == {
        'id': [1, 1, 2, 3],
        'name': ['y', 'y', 'c', 'd'],
        'score': [101, 101, 20, 30],
    }

    stream = table.new_stream_write_builder()
    next_rows = pa.record_batch([
        pa.array([2, 4], type=pa.int32()),
        pa.array(['C', 'e']),
        pa.array([21, 40], type=pa.int32()),
    ], names=['id', 'name', 'score'])
    stream_update = stream.new_update().with_update_type(
        update_cols=['name', 'score'])
    assert not hasattr(stream_update, 'close')
    assert not hasattr(stream_update, 'add_matched_batch')
    messages = stream_update.upsert_by_arrow_with_key(
        table=pa.Table.from_batches([next_rows]), upsert_keys=['id'],
        commit_identifier=42)
    stream.new_commit().commit(42, messages)
    next_round = pa.record_batch([
        pa.array([4], type=pa.int32()),
        pa.array(['E']),
        pa.array([41], type=pa.int32()),
    ], names=['id', 'name', 'score'])
    messages = stream_update.upsert_by_arrow_with_key(
        pa.Table.from_batches([next_round]), ['id'], 43)
    stream.new_commit().commit(43, messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.table_upsert.t'))
    actual = actual.sort_by([('id', 'ascending'), ('name', 'ascending')]).to_pydict()
    assert actual == {
        'id': [1, 1, 2, 3, 4],
        'name': ['y', 'y', 'C', 'd', 'E'],
        'score': [101, 101, 21, 30, 41],
    }

    builder = table.new_batch_write_builder()
    all_columns = builder.new_update()
    messages = all_columns.upsert_by_arrow_with_key(pa.Table.from_pydict({
        'id': [3], 'name': ['D'], 'score': [31],
    }, schema=pa.schema([
        ('id', pa.int32()), ('name', pa.string()), ('score', pa.int32()),
    ])), ['id'])
    builder.new_commit().commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.table_upsert.t'))
    actual = actual.sort_by([('id', 'ascending'), ('name', 'ascending')]).to_pydict()
    assert actual == {
        'id': [1, 1, 2, 3, 4],
        'name': ['y', 'y', 'C', 'D', 'E'],
        'score': [101, 101, 21, 31, 41],
    }

    # Empty selection means all columns for upsert, as in PyPaimon.
    # Invalid configuration must not overwrite the previous selection.
    all_columns.with_update_type([])
    with pytest.raises(ValueError, match='not in table schema'):
        all_columns.with_update_type(['missing'])
    messages = all_columns.upsert_by_arrow_with_key(
        pa.Table.from_batches([first]), ['id'])
    builder.new_commit().commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name, score FROM paimon.table_upsert.t WHERE id = 1'))
    assert actual.to_pydict() == {'id': [1, 1], 'name': ['x', 'x'], 'score': [100, 100]}
    with pytest.raises(ValueError, match='column_names cannot be empty'):
        all_columns.update_by_arrow_with_row_id(
            pa.table({'_ROW_ID': [0], 'score': [2]}))


def test_batch_update_row_ids_commits_through_write_builder(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.updates')
    context.sql("""CREATE TABLE paimon.updates.t (id INT, name STRING) WITH (
        'row-tracking.enabled' = 'true',
        'data-evolution.enabled' = 'true')""")
    context.sql("""INSERT INTO paimon.updates.t (id, name)
        VALUES (1, 'a'), (2, 'b'), (3, 'c')""")

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('updates.t')
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['id'])
    row_ids = update.new_update_by_row_id()
    messages = row_ids.update_columns(pa.table({
        '_ROW_ID': [0, 2], 'name': ['A', 'C'],
    }), ['name'])
    assert messages and messages[0].serialize()
    assert [m.serialize() for m in row_ids.commit_messages] == [m.serialize() for m in messages]
    with pytest.raises(ValueError, match='overlapping first_row_ids'):
        row_ids.update_columns(pa.table({'_ROW_ID': [1], 'name': ['B']}), ['name'])
    builder.new_commit().commit(messages)

    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3], 'name': ['A', 'b', 'C']}


def test_grouped_batch_update_checks_input_table_file_overlap(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.grouped_updates')
    context.sql("""CREATE TABLE paimon.grouped_updates.t (id INT, name STRING) WITH (
        'row-tracking.enabled' = 'true',
        'data-evolution.enabled' = 'true')""")
    context.sql("""INSERT INTO paimon.grouped_updates.t (id, name)
        VALUES (1, 'a'), (2, 'b')""")
    context.sql("""INSERT INTO paimon.grouped_updates.t (id, name)
        VALUES (3, 'c'), (4, 'd')""")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table(
        'grouped_updates.t')

    before = set(tmp_path.rglob('*.parquet'))
    snapshot_id = table.latest_snapshot().id()
    overlap = table.new_batch_write_builder().new_update()
    with pytest.raises(ValueError, match='overlapping first_row_ids.*0'):
        overlap.update_by_arrow_batches_with_row_id(iter([
            pa.table({'_ROW_ID': [0], 'name': ['A']}),
            pa.table({'_ROW_ID': [1], 'name': ['B']}),
        ]))
    assert set(tmp_path.rglob('*.parquet')) > before
    assert table.latest_snapshot().id() == snapshot_id
    before = set(tmp_path.rglob('*.parquet'))

    def failing_tables():
        yield pa.table({'_ROW_ID': [0], 'name': ['A']})
        raise RuntimeError('input failed')

    with pytest.raises(RuntimeError, match='input failed'):
        overlap.update_by_arrow_batches_with_row_id(failing_tables())
    assert set(tmp_path.rglob('*.parquet')) > before
    assert table.latest_snapshot().id() == snapshot_id
    unchanged = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.grouped_updates.t')).sort_by('id').to_pydict()
    assert unchanged == {'id': [1, 2, 3, 4], 'name': ['a', 'b', 'c', 'd']}

    builder = table.new_batch_write_builder()
    messages = builder.new_update().update_by_arrow_batches_with_row_id(iter([
        pa.Table.from_batches([
            pa.record_batch([pa.array([0], type=pa.int64()), pa.array(['A'])],
                            names=['_ROW_ID', 'name']),
            pa.record_batch([pa.array([1], type=pa.int64()), pa.array(['B'])],
                            names=['_ROW_ID', 'name']),
        ]),
        pa.table({'_ROW_ID': [2], 'name': ['C']}),
    ]))
    builder.new_commit().commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.grouped_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3, 4], 'name': ['A', 'B', 'C', 'd']}

    def creates_rows_before_first_yield():
        context.sql("INSERT INTO paimon.grouped_updates.t (id, name) VALUES (5, 'e')")
        yield pa.table({'_ROW_ID': [4], 'name': ['E']})

    builder = table.new_batch_write_builder()
    messages = builder.new_update().update_by_arrow_batches_with_row_id(
        creates_rows_before_first_yield())
    builder.new_commit().commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.grouped_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3, 4, 5], 'name': ['A', 'B', 'C', 'd', 'E']}

    context.sql('CREATE TABLE paimon.grouped_updates.plain (id INT)')
    plain = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('grouped_updates.plain')
    assert plain.new_batch_write_builder().new_update().update_by_arrow_batches_with_row_id(
        iter([])) == []


def test_batch_delete_row_ids_commits_deletion_vectors(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.deletes')
    context.sql("""CREATE TABLE paimon.deletes.t (id INT, name STRING) WITH (
        'row-tracking.enabled' = 'true',
        'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    context.sql("""INSERT INTO paimon.deletes.t (id, name)
        VALUES (1, 'a'), (2, 'b'), (3, 'c')""")

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('deletes.t')
    builder = table.new_batch_write_builder()
    update = builder.new_update()
    messages = update.delete_by_row_id([0, 2, 2])
    assert messages and messages[0].serialize()
    builder.new_commit().commit(messages)

    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.deletes.t')).to_pydict()
    assert actual == {'id': [2], 'name': ['b']}

    stream = table.new_stream_write_builder()
    stream_update = stream.new_update()
    stream.new_commit().commit(42, stream_update.delete_by_row_id([1], 42))
    assert list(context.sql('SELECT id, name FROM paimon.deletes.t')) == []


def test_stream_row_id_update_and_factory(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.stream_updates')
    context.sql("""CREATE TABLE paimon.stream_updates.t (id INT, value INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    context.sql('INSERT INTO paimon.stream_updates.t (id, value) VALUES (1, 10), (2, 20)')
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('stream_updates.t')
    stream = table.new_stream_write_builder()
    update = stream.new_update()
    messages = update.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'value': [11]}), 10)
    stream.new_commit().commit(10, messages)
    low = update.new_update_by_row_id(11)
    with pytest.raises(ValueError, match='must contain _ROW_ID'):
        low.update_columns(pa.table({'value': pa.array([], type=pa.int32())}), ['value'])
    messages = low.update_columns(pa.table({'_ROW_ID': [1], 'value': [22]}), ['value'])
    stream.new_commit().commit(11, messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, value FROM paimon.stream_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2], 'value': [11, 22]}


def test_predicate_update_owns_scan_callbacks_and_preserves_prepared_files(tmp_path):
    assert not hasattr(datafusion, '_MatchedBatchUpdateWriter')
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.pred_updates')
    context.sql("""CREATE TABLE paimon.pred_updates.t (id INT, value INT, score INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    for values in ('(1, 10, 100), (2, 20, 200)', '(3, 30, 300), (4, 40, 400)'):
        context.sql('INSERT INTO paimon.pred_updates.t (id, value, score) VALUES ' + values)
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('pred_updates.t')
    builder = table.new_batch_write_builder()
    assert not hasattr(builder, '_new_matched_update')
    update = builder.new_update().with_update_type(['id'])
    seen = []

    def increment(rows):
        assert rows.column_names == ['value', '_ROW_ID']
        seen.append(rows.num_rows)
        return pa.compute.add(rows['value'], 1)

    messages = update.update_by_predicate(
        {'method': 'greaterOrEqual', 'field': 'id', 'literals': [2]},
        {'value': increment, 'score': 999}, read_columns=['value'])
    assert sorted(seen) == [1, 2]
    builder.new_commit().commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, value, score FROM paimon.pred_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3, 4], 'value': [10, 21, 31, 41],
                      'score': [100, 999, 999, 999]}

    before = set(tmp_path.rglob('*.parquet'))
    snapshot_id = table.latest_snapshot().id()
    seen.clear()

    def fail_second(rows):
        seen.append(rows.num_rows)
        if len(seen) == 2:
            raise RuntimeError('callback failure')
        return rows['value']

    with pytest.raises(RuntimeError, match='callback failure'):
        update.update_by_predicate(None, {'value': fail_second}, read_columns=['value'])
    assert len(seen) == 2
    assert set(tmp_path.rglob('*.parquet')) > before
    assert table.latest_snapshot().id() == snapshot_id
    unchanged = pa.Table.from_batches(context.sql(
        'SELECT id, value, score FROM paimon.pred_updates.t')).sort_by('id').to_pydict()
    assert unchanged == actual
    assert update.update_by_predicate(
        {'method': 'equal', 'field': 'id', 'literals': [99]},
        {'value': 'bad-int'}) == []
    assert update.update_by_predicate(
        {'method': 'equal', 'field': 'id', 'literals': [99]},
        {'value': fail_second}, read_columns=['value']) == []
    assert len(seen) == 2

    stream = table.new_stream_write_builder()
    messages = stream.new_update().update_by_predicate(
        None, {'score': pa.chunked_array([[11], [22, 33, 44]])}, 42)
    stream.new_commit().commit(42, messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, score FROM paimon.pred_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3, 4], 'score': [11, 22, 33, 44]}


@pytest.mark.parametrize('kind', ['batch', 'stream', 'row_id'])
def test_update_can_transfer_and_be_collected_on_another_thread(tmp_path, kind, monkeypatch):
    import gc
    import sys
    from concurrent.futures import ThreadPoolExecutor

    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.threaded_updates')
    context.sql("""CREATE TABLE paimon.threaded_updates.t (id INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('threaded_updates.t')
    builder = table.new_stream_write_builder() if kind == 'stream' else table.new_batch_write_builder()
    update = builder.new_update()
    if kind == 'row_id':
        update = update.new_update_by_row_id()

    def access(value):
        if kind == 'row_id':
            return value.commit_messages
        value.with_update_type(['id'])
        rows = value.new_update_by_row_id(42) if kind == 'stream' else value.new_update_by_row_id()
        return rows.commit_messages

    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(access, update).result() == []
        # Exception tracebacks and wrapper cycles can defer destruction to GC
        # on a different thread, even if operations stay on the creator thread.
        gc.collect()
        errors = []
        monkeypatch.setattr(sys, 'unraisablehook', errors.append)
        enabled = gc.isenabled()
        gc.disable()
        try:
            cycle = [update]
            cycle.append(cycle)
            del update, cycle
            pool.submit(gc.collect).result()
            assert errors == []
        finally:
            if enabled:
                gc.enable()


@pytest.mark.parametrize('stream', [False, True])
def test_prepared_merge_conditions_chunks_and_lazy_literals(tmp_path, stream):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.prepared_merge')
    context.sql("""CREATE TABLE paimon.prepared_merge.t (id INT, value INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    # Separate commits force the clause to consume values across target files.
    context.sql('INSERT INTO paimon.prepared_merge.t (id, value) VALUES (1, 10)')
    context.sql('INSERT INTO paimon.prepared_merge.t (id, value) VALUES (2, 20)')
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('prepared_merge.t')
    builder = (table.new_stream_write_builder().with_commit_user('prepared-merge')
               if stream else table.new_batch_write_builder())
    source = pa.table({'id': [1, 2], 'value': [11, 22]}, schema=pa.schema([
        ('id', pa.int32()), ('value', pa.int32())]))
    update = builder.new_update()
    kwargs = dict(commit_identifier=42) if stream else {}
    false_clause = WhenMatched.update(
        {'value': MergeAssignment.literal('invalid int')}, condition='"s.value" < 0')
    assert update.merge_into(source, on=[('id', 'id')], when_matched=[false_clause],
                             when_not_matched=[], **kwargs) == []
    messages = update.merge_into(source, on=[('id', 'id')], when_matched=[
        WhenMatched.update(
            {'value': MergeAssignment.literal(pa.chunked_array([[100], [200]]))},
            condition='"s.value" IN (SELECT v FROM (VALUES (11), (22)) AS m(v))')],
        when_not_matched=[], **kwargs)
    # Preparation leaves the visible snapshot unchanged.

    def rows():
        return pa.Table.from_batches(context.sql(
            'SELECT id, value FROM paimon.prepared_merge.t ORDER BY id')).to_pylist()
    assert rows() == [dict(id=1, value=10), dict(id=2, value=20)]
    commit = builder.new_commit()
    commit.commit(42, messages) if stream else commit.commit(messages)
    assert rows() == [dict(id=1, value=100), dict(id=2, value=200)]


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('historical', [False, True])
def test_prepared_merge_accepts_native_table_sources(tmp_path, stream, historical):
    import json
    from pathlib import Path

    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.table_merge')
    context.sql("""CREATE TABLE paimon.table_merge.target (id INT, value INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    # An ordinary append source needs neither row tracking nor data evolution.
    context.sql('CREATE TABLE paimon.table_merge.source (source_id INT, amount INT, flag INT)')
    context.sql('INSERT INTO paimon.table_merge.target (id, value) VALUES (1, 10), (2, 20)')
    for values in ['(1, 11, 1)', '(3, 33, 1)', '(2, 22, 1)']:
        context.sql('INSERT INTO paimon.table_merge.source (source_id, amount, flag) VALUES ' + values)
    catalog = PaimonCatalog({'warehouse': str(tmp_path)})
    target, source = catalog.get_table('table_merge.target'), catalog.get_table('table_merge.source')
    if historical:
        schema = json.loads((Path(source.location()) / 'schema' / 'schema-0').read_text())
        schema['options']['scan.snapshot-id'] = '2'
        source = source.copy_with_resolved_schema(json.dumps(schema))
    source_snapshot = source.latest_snapshot().id()
    target_snapshot = target.latest_snapshot().id()
    builder = (target.new_stream_write_builder().with_commit_user('source-merge')
               if stream else target.new_batch_write_builder())
    kwargs = dict(commit_identifier=42) if stream else {}
    condition = '"s.flag" = 1'
    messages = builder.new_update().merge_into(
        source, on=[('id', 'source_id')], when_matched=[
            WhenMatched.update({'value': MergeAssignment.source_column('amount')}, condition)],
        when_not_matched=[WhenNotMatched({
            'id': MergeAssignment.source_column('source_id'),
            'value': MergeAssignment.source_column('amount'),
        }, condition)], **kwargs)
    assert target.latest_snapshot().id() == target_snapshot
    assert source.latest_snapshot().id() == source_snapshot
    commit = builder.new_commit()
    commit.commit(42, messages) if stream else commit.commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, value FROM paimon.table_merge.target ORDER BY id')).to_pylist()
    assert actual == [dict(id=1, value=11), dict(id=2, value=20 if historical else 22), dict(id=3, value=33)]


@pytest.mark.parametrize('stream', [False, True])
def test_prepared_merge_callable_uses_complete_clause_and_preserves_exception(tmp_path, stream):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.callable_merge')
    context.sql("""CREATE TABLE paimon.callable_merge.t (id INT, value INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    for values in ['(1, 10)', '(2, 20)']:
        context.sql('INSERT INTO paimon.callable_merge.t (id, value) VALUES ' + values)
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('callable_merge.t')
    builder = (table.new_stream_write_builder().with_commit_user('callable-merge')
               if stream else table.new_batch_write_builder())
    kwargs = dict(commit_identifier=42) if stream else {}
    update = builder.new_update()
    source = pa.table({'id': pa.array([1, 2, 3], pa.int32()), 'delta': pa.array([4, 5, 6], pa.int32())})
    calls = []

    def values(batch):
        calls.append(batch.num_rows)
        assert {'t.id', 't.value', 's.id', 's.delta'} <= set(batch.column_names)
        return pa.chunked_array([[100], [200]], type=pa.int32())

    messages = update.merge_into(source, on=[('id', 'id')], when_matched=[
        WhenMatched.update({'value': MergeAssignment.function(values)})], when_not_matched=[
        WhenNotMatched({'id': MergeAssignment.source_column('id'),
                        'value': MergeAssignment.function(lambda batch: batch['s.delta'])})],
        **kwargs)
    assert calls == [2]
    commit = builder.new_commit()
    commit.commit(42, messages) if stream else commit.commit(messages)
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, value FROM paimon.callable_merge.t ORDER BY id')).to_pylist()
    assert actual == [dict(id=1, value=100), dict(id=2, value=200), dict(id=3, value=6)]
    error = RuntimeError('callback error')

    def fail(batch):
        raise error

    clause = [WhenMatched.update({'value': MergeAssignment.function(fail)})]
    update = builder.new_update()
    assert update.merge_into(pa.table({'id': pa.array([99], pa.int32())}), on=[('id', 'id')],
                             when_matched=clause, when_not_matched=[], **kwargs) == []
    with pytest.raises(RuntimeError) as caught:
        update.merge_into(source, on=[('id', 'id')], when_matched=clause,
                          when_not_matched=[], **kwargs)
    assert caught.value is error
