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

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext, UpsertKeyMatcher


def test_upsert_key_matcher_deduplicates_and_fans_out():
    source = pa.record_batch([
        pa.array([1, 1, 2, None, 3], type=pa.int32()),
        pa.array(['a', 'a', 'b', 'n', 'c']),
    ], names=['id', 'part'])
    matcher = UpsertKeyMatcher(source, ['id', 'part'])
    assert matcher.deduplicated_indices() == [1, 2, 3, 4]
    matcher.add_existing_batch(pa.record_batch([
        pa.array([1, 2, 1, None, 9], type=pa.int32()),
        pa.array(['a', 'b', 'a', 'n', 'other']),
        pa.array([10, 20, 11, 30, 40], type=pa.int64()),
    ], names=['id', 'part', '_ROW_ID']))
    assert matcher.finish() == ([1, 1, 2, 3], [10, 11, 20, 30], [4])


def test_upsert_key_matcher_rejects_different_key_types():
    matcher = UpsertKeyMatcher(pa.record_batch([
        pa.array([1], type=pa.int32()),
    ], names=['id']), ['id'])
    with pytest.raises(ValueError, match='upsert key type differs'):
        matcher.add_existing_batch(pa.record_batch([
            pa.array([1], type=pa.int64()),
            pa.array([0], type=pa.int64()),
        ], names=['id', '_ROW_ID']))


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
    update = builder.new_update(['name'])
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
    delete = builder.new_delete()
    delete.add_row_ids([0, 2, 2])
    messages = delete.prepare_commit()
    assert messages and messages[0].serialize()
    with pytest.raises(RuntimeError, match='closed'):
        delete.add_row_ids([1])
    builder.new_commit().commit(messages)

    actual = pa.Table.from_batches(context.sql(
        'SELECT id, name FROM paimon.deletes.t')).to_pydict()
    assert actual == {'id': [2], 'name': ['b']}
