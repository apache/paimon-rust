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

"""Per-snapshot core planning through the Python wrapper."""

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SnapshotReader, SQLContext


@pytest.fixture
def table(tmp_path):
    options = {"warehouse": str(tmp_path)}
    ctx = SQLContext()
    ctx.register_catalog("paimon", options)
    ctx.sql("CREATE SCHEMA paimon.snapdb")
    ctx.sql("CREATE TABLE paimon.snapdb.t (id INT, value INT, PRIMARY KEY (id)) "
            "WITH ('bucket'='4', 'changelog-producer'='input', "
            "'source.split.target-size'='1 b', 'source.split.open-file-cost'='1 b')")
    table = PaimonCatalog(options).get_table("snapdb.t")
    for value in (100, 200):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.record_batch({'id': pa.array(range(32), type=pa.int32()),
                                     'value': pa.array(range(value, value + 32), type=pa.int32())}))
        builder.new_commit().commit(writer.prepare_commit())
        writer.close()
    return table


def _rows(builder, plan):
    return sorted((row['id'], row['value'])
                  for batch in builder.new_read().read(plan.splits()) for row in batch.to_pylist())


@pytest.mark.parametrize('mode', ['all', 'delta', 'changelog'])
def test_snapshot_id_and_modes_survive_repeated_reads(table, mode):
    builder = table.new_read_builder()
    reader = builder.new_snapshot_reader().with_snapshot(1).with_mode(mode)
    assert isinstance(reader, SnapshotReader)
    for _ in range(2):
        plan = reader.read()
        assert plan.snapshot_id() == 1
        assert _rows(builder, plan) == [(i, 100 + i) for i in range(32)]
        assert all(split.is_streaming() == (mode != 'all') for split in plan.splits())
    reader.with_snapshot(2)
    assert _rows(builder, reader.read()) == [(i, 200 + i) for i in range(32)]
    assert _rows(builder, table.new_snapshot_reader().read()) == [(i, 200 + i) for i in range(32)]


@pytest.mark.parametrize('mode', ['all', 'delta', 'changelog'])
def test_bucket_callback_replacement_clearing_and_shard(table, mode):
    builder = table.new_read_builder()
    reader = builder.new_snapshot_reader().with_mode(mode).with_bucket_filter(lambda bucket: False)
    assert reader.read().splits() == []
    reader.with_bucket_filter(lambda bucket: bucket % 2 == 0)
    selected = reader.read()
    selected_rows = _rows(builder, selected)
    assert 0 < len(selected_rows) < 32
    shard = builder.new_snapshot_reader().with_mode(mode).with_shard(0, 2).read()
    assert selected_rows == _rows(builder, shard)
    reader.with_bucket_filter(None)
    assert len(_rows(builder, reader.read())) == 32
    union = []
    for index in range(2):
        plan = builder.new_snapshot_reader().with_mode(mode).with_shard(index, 2).read()
        union.extend(_rows(builder, plan))
    assert sorted(union) == [(i, 200 + i) for i in range(32)]


@pytest.mark.parametrize('mode', ['all', 'delta', 'changelog'])
def test_callback_exception_is_preserved_without_retry(table, mode):
    calls = []
    failure = LookupError('bucket callback failed')

    def select(bucket):
        calls.append(bucket)
        if len(calls) == 2:
            raise failure
        return True

    reader = table.new_snapshot_reader().with_mode(mode).with_bucket_filter(select)
    with pytest.raises(LookupError) as caught:
        reader.read()
    assert caught.value is failure
    assert len(calls) == 2


@pytest.mark.parametrize('mode', ['all', 'delta', 'changelog'])
def test_projection_and_predicate_come_from_read_builder(table, mode):
    builder = (table.new_read_builder().with_projection(['value']).with_filter({
        'method': 'equal', 'field': 'id', 'literals': [2]}))
    plan = builder.new_snapshot_reader().with_mode(mode).with_snapshot(1).read()
    assert [row for batch in builder.new_read().read(plan.splits()) for row in batch.to_pylist()] == [
        {'value': 102}]


@pytest.mark.parametrize('mode', ['all', 'delta', 'changelog'])
def test_missing_snapshot_is_not_hidden_by_empty_limit_or_bucket_filter(table, mode):
    reader = (table.new_read_builder().with_limit(0).new_snapshot_reader().with_mode(mode)
              .with_snapshot(12345).with_bucket_filter(lambda bucket: False))
    with pytest.raises(ValueError, match='12345'):
        reader.read()


def test_invalid_configuration_fails_at_setter(table):
    reader = table.new_snapshot_reader()
    for value in [0, -1]:
        with pytest.raises(ValueError, match='snapshot id'):
            reader.with_snapshot(value)
    for mode in ['auto', 'diff', 'latest', '']:
        with pytest.raises(ValueError, match='snapshot mode'):
            reader.with_mode(mode)
    for index, count in [(0, 0), (2, 2)]:
        with pytest.raises(ValueError, match='shard'):
            reader.with_shard(index, count)
    with pytest.raises(TypeError, match='callable'):
        reader.with_bucket_filter(42)
