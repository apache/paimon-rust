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

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


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
    overlap = table.new_batch_write_builder().new_update()
    with pytest.raises(ValueError, match='overlapping first_row_ids.*0'):
        overlap.update_by_arrow_batches_with_row_id(iter([
            pa.table({'_ROW_ID': [0], 'name': ['A']}),
            pa.table({'_ROW_ID': [1], 'name': ['B']}),
        ]))
    assert set(tmp_path.rglob('*.parquet')) == before

    def failing_tables():
        yield pa.table({'_ROW_ID': [0], 'name': ['A']})
        raise RuntimeError('input failed')

    with pytest.raises(RuntimeError, match='input failed'):
        overlap.update_by_arrow_batches_with_row_id(failing_tables())
    assert set(tmp_path.rglob('*.parquet')) == before

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


def test_predicate_update_owns_scan_callbacks_and_rollback(tmp_path):
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
    seen.clear()

    def fail_second(rows):
        seen.append(rows.num_rows)
        if len(seen) == 2:
            raise RuntimeError('callback failure')
        return rows['value']

    with pytest.raises(RuntimeError, match='callback failure'):
        update.update_by_predicate(None, {'value': fail_second}, read_columns=['value'])
    assert len(seen) == 2
    assert set(tmp_path.rglob('*.parquet')) == before
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
