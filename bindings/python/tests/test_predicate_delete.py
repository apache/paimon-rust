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

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('partition_delete', [False, True])
def test_predicate_delete_returns_messages_for_batch_and_stream(tmp_path, stream, partition_delete):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.deletes')
    context.sql("""CREATE TABLE paimon.deletes.t (id INT, p STRING, v INT) PARTITIONED BY (p) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = '%s')""" % ('false' if partition_delete else 'true'))
    context.sql("INSERT INTO paimon.deletes.t (id, p, v) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('deletes.t')
    builder = (table.new_stream_write_builder().with_commit_user('predicate-stream') if stream
               else table.new_batch_write_builder())
    predicate = {'method': 'equal', 'field': 'p' if partition_delete else 'v',
                 'literals': ['a' if partition_delete else 20]}
    update = builder.new_update()
    messages = (update.delete_by_predicate(predicate, 7) if stream
                else update.delete_by_predicate(predicate))
    assert messages and all(message.serialize() for message in messages)
    # Preparation alone does not publish the delete.
    assert pa.Table.from_batches(context.sql('SELECT id FROM paimon.deletes.t')).num_rows == 3
    commit = builder.new_commit()
    commit.commit(7, messages) if stream else commit.commit(messages)
    assert pa.Table.from_batches(context.sql(
        'SELECT id FROM paimon.deletes.t ORDER BY id'))['id'].to_pylist() == (
        [3] if partition_delete else [1, 3])


def test_predicate_delete_requires_explicit_selection_but_none_matches_all(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.deletes')
    context.sql("""CREATE TABLE paimon.deletes.t (id INT) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'deletion-vectors.enabled' = 'true')""")
    context.sql('INSERT INTO paimon.deletes.t (id) VALUES (1), (2)')
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('deletes.t')
    builder = table.new_batch_write_builder()
    update = builder.new_update()
    with pytest.raises(TypeError, match='predicate'):
        update.delete_by_predicate()
    messages = update.delete_by_predicate(None)
    assert messages
    builder.new_commit().commit(messages)
    assert list(context.sql('SELECT id FROM paimon.deletes.t')) == []
