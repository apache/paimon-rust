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
#

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _register(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    return context


def _bucket_files(table_dir, partition=None):
    base = table_dir / partition if partition else table_dir
    return sorted(str(p) for p in base.glob('**/bucket-*/*'))


def _assert_compact_snapshot(table, previous):
    snapshot = table.latest_snapshot()
    assert snapshot.id() == previous + 1
    assert snapshot.commit_kind() == 'COMPACT'


def _write_primary_key_table(tmp_path, options=None):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    options = {'bucket': '1', 'write-only': 'true', **(options or {})}
    properties = ', '.join(f"'{name}' = '{value}'" for name, value in options.items())
    context.sql(
        f'CREATE TABLE paimon.db.t (id INT, v INT, PRIMARY KEY (id)) WITH ({properties})')
    context.sql('INSERT INTO paimon.db.t VALUES (1, 10), (2, 20)')
    context.sql('INSERT INTO paimon.db.t VALUES (1, 11)')


def _read_rows(table):
    read = table.new_read_builder()
    return sorted((row['id'], row['v'])
                  for batch in read.new_read().read(read.new_scan().plan().splits())
                  for row in batch.to_pylist())


def test_compact_builder_class_is_importable():
    from pypaimon_rust.datafusion import CompactBuilder

    assert CompactBuilder.__name__ == 'CompactBuilder'


def test_compact_merges_append_files_and_preserves_rows(tmp_path):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    context.sql("""CREATE TABLE paimon.db.t (id INT, p STRING, v INT) PARTITIONED BY (p) WITH (
        'target-file-size' = '1mb', 'compaction.min.file-num' = '2',
        'write-only' = 'true')""")
    for i in range(3):
        context.sql(
            f"INSERT INTO paimon.db.t VALUES ({i}, 'a', {i * 10}), ({i + 3}, 'a', {i * 10 + 1})")

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    table_dir = tmp_path / 'db.db' / 't'
    assert len(_bucket_files(table_dir)) >= 3
    snapshot = table.latest_snapshot().id()

    metrics = table.new_compact_builder().execute()

    assert metrics['tasks'] >= 1
    assert metrics['files_removed'] >= 2
    assert metrics['files_added'] >= 1
    assert metrics['files_removed'] > metrics['files_added']
    _assert_compact_snapshot(table, snapshot)
    result = pa.Table.from_batches(context.sql('SELECT id, v FROM paimon.db.t ORDER BY id'))
    assert result['id'].to_pylist() == [0, 1, 2, 3, 4, 5]
    assert result['v'].to_pylist() == [0, 10, 20, 1, 11, 21]


def test_compact_without_tasks_makes_no_commit(tmp_path):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    context.sql("""CREATE TABLE paimon.db.t (id INT) WITH (
        'target-file-size' = '1mb', 'compaction.min.file-num' = '2',
        'write-only' = 'true')""")
    context.sql('INSERT INTO paimon.db.t VALUES (1), (2)')
    context.sql('INSERT INTO paimon.db.t VALUES (3)')

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    first = table.new_compact_builder().execute()
    assert first['tasks'] >= 1
    snapshot = table.latest_snapshot().id()

    second = table.new_compact_builder().execute()

    assert second['tasks'] == 0
    assert second['files_removed'] == 0
    assert second['files_added'] == 0
    assert table.latest_snapshot().id() == snapshot


def test_compact_partition_filter_leaves_other_partitions_alone(tmp_path):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    context.sql("""CREATE TABLE paimon.db.t (id INT, p STRING) PARTITIONED BY (p) WITH (
        'target-file-size' = '1mb', 'compaction.min.file-num' = '2',
        'write-only' = 'true')""")
    for i in range(2):
        context.sql(f"INSERT INTO paimon.db.t VALUES ({i}, 'a'), ({i}, 'b')")

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    table_dir = tmp_path / 'db.db' / 't'
    files_before_a = _bucket_files(table_dir, 'p=a')
    files_before_b = _bucket_files(table_dir, 'p=b')
    snapshot = table.latest_snapshot().id()

    metrics = (
        table.new_compact_builder()
        .with_partition_filter({'method': 'equal', 'field': 'p', 'literals': ['a']})
        .execute()
    )

    assert metrics['tasks'] >= 1
    assert metrics['files_removed'] >= 2
    _assert_compact_snapshot(table, snapshot)
    assert set(files_before_a) < set(_bucket_files(table_dir, 'p=a'))
    assert _bucket_files(table_dir, 'p=b') == files_before_b
    result = pa.Table.from_batches(context.sql('SELECT id, p FROM paimon.db.t ORDER BY p, id'))
    assert result['id'].to_pylist() == [0, 1, 0, 1]
    assert result['p'].to_pylist() == ['a', 'a', 'b', 'b']


def test_compact_with_commit_user_records_identity(tmp_path):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    context.sql("""CREATE TABLE paimon.db.t (id INT) WITH (
        'target-file-size' = '1mb', 'compaction.min.file-num' = '2',
        'write-only' = 'true')""")
    context.sql('INSERT INTO paimon.db.t VALUES (1), (2)')
    context.sql('INSERT INTO paimon.db.t VALUES (3)')

    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    metrics = (
        table.new_compact_builder()
        .with_commit_user('python-compactor')
        .execute()
    )

    assert metrics['tasks'] >= 1
    assert table.latest_snapshot().commit_user().startswith('python-compactor-compact-')
    result = pa.Table.from_batches(context.sql('SELECT id FROM paimon.db.t ORDER BY id'))
    assert result['id'].to_pylist() == [1, 2, 3]


def test_compact_builder_validates_arguments_when_set(tmp_path):
    context = _register(tmp_path)
    context.sql('CREATE SCHEMA paimon.db')
    context.sql('CREATE TABLE paimon.db.t (id INT, p STRING) PARTITIONED BY (p)')
    builder = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t').new_compact_builder()

    with pytest.raises(ValueError, match='partition keys'):
        builder.with_partition_filter({'method': 'equal', 'field': 'id', 'literals': [1]})
    with pytest.raises(ValueError, match='commit_user'):
        builder.with_commit_user(commit_user='a/b')


def test_compact_merges_primary_key_sorted_runs(tmp_path):
    _write_primary_key_table(tmp_path)
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    snapshot = table.latest_snapshot().id()

    metrics = table.new_compact_builder().execute()

    assert metrics['tasks'] == 1
    assert metrics['files_removed'] == 2
    assert metrics['files_added'] == 1
    _assert_compact_snapshot(table, snapshot)
    assert _read_rows(table) == [(1, 11), (2, 20)]


def test_compact_deletion_vector_table_requires_local_cache(tmp_path):
    _write_primary_key_table(tmp_path, {'deletion-vectors.enabled': 'true'})
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.t')
    snapshot = table.latest_snapshot().id()

    with pytest.raises(ValueError, match='local-cache.enabled'):
        table.new_compact_builder().execute()
    assert table.latest_snapshot().id() == snapshot

    cached = PaimonCatalog({'warehouse': str(tmp_path), 'local-cache.enabled': 'true'})
    table = cached.get_table('db.t')
    # Write-only deletion-vector tables expose level-0 rows only after compaction.
    assert _read_rows(table) == []
    table.new_compact_builder().execute()
    _assert_compact_snapshot(table, snapshot)
    assert _read_rows(table) == [(1, 11), (2, 20)]
