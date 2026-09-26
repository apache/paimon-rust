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

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext, _match_upsert_keys


def test_upsert_key_matcher_deduplicates_and_fans_out():
    assert not hasattr(datafusion, 'UpsertKeyMatcher')
    assert not hasattr(datafusion, 'BatchTableDelete')
    source = pa.record_batch([
        pa.array([1, 1, 2, None, 3], type=pa.int32()),
        pa.array(['a', 'a', 'b', 'n', 'c']),
    ], names=['id', 'part'])
    existing = pa.record_batch([
        pa.array([1, 2, 1, None, 9], type=pa.int32()),
        pa.array(['a', 'b', 'a', 'n', 'other']),
        pa.array([10, 20, 11, 30, 40], type=pa.int64()),
    ], names=['id', 'part', '_ROW_ID'])
    assert _match_upsert_keys(source, ['id', 'part'], iter([existing])) == (
        [1, 1, 2, 3], [10, 11, 20, 30], [4]
    )


def test_upsert_key_matcher_rejects_different_key_types():
    source = pa.record_batch([
        pa.array([1], type=pa.int32()),
    ], names=['id'])
    existing = pa.record_batch([
        pa.array([1], type=pa.int64()),
        pa.array([0], type=pa.int64()),
    ], names=['id', '_ROW_ID'])
    with pytest.raises(ValueError, match='upsert key type differs'):
        _match_upsert_keys(source, ['id'], [existing])


def test_table_upsert_updates_duplicate_targets_and_appends(tmp_path):
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
    update = builder._new_matched_update(['name'])
    update.add_matched_batch(pa.record_batch([
        pa.array([0, 2], type=pa.int64()),
        pa.array(['A', 'C']),
    ], names=['_ROW_ID', 'name']))
    messages = update.prepare_commit()
    assert messages and messages[0].serialize()
    with pytest.raises(RuntimeError, match='closed'):
        update.prepare_commit()
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

    overlap = table.new_batch_write_builder()._new_matched_update(['name'])
    overlap.add_matched_group([pa.record_batch([
        pa.array([0], type=pa.int64()), pa.array(['A']),
    ], names=['_ROW_ID', 'name'])])
    overlap.add_matched_group([pa.record_batch([
        pa.array([1], type=pa.int64()), pa.array(['B']),
    ], names=['_ROW_ID', 'name'])])
    with pytest.raises(ValueError, match='overlapping first_row_ids.*0'):
        overlap.prepare_commit()

    builder = table.new_batch_write_builder()
    update = builder._new_matched_update(['name'])
    update.add_matched_group([
        pa.record_batch([
            pa.array([0], type=pa.int64()), pa.array(['A']),
        ], names=['_ROW_ID', 'name']),
        pa.record_batch([
            pa.array([1], type=pa.int64()), pa.array(['B']),
        ], names=['_ROW_ID', 'name']),
    ])
    update.add_matched_group([pa.record_batch([
        pa.array([2], type=pa.int64()), pa.array(['C']),
    ], names=['_ROW_ID', 'name'])])
    builder.new_commit().commit(update.prepare_commit())
    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.grouped_updates.t')).sort_by('id').to_pydict()
    assert actual == {'id': [1, 2, 3, 4], 'name': ['A', 'B', 'C', 'd']}


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
