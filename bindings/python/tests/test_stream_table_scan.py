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

"""Stateful core streaming through the Python wrapper."""

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, StreamTableScan, SQLContext


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


@pytest.mark.parametrize('follow_up', [False, True])
def test_cursor_restore_and_initial_vs_follow_up(table, follow_up):
    builder = table.new_read_builder()
    scan = builder.new_stream_scan()
    assert isinstance(scan, StreamTableScan)
    assert scan.checkpoint() is None
    if follow_up:
        scan.restore(1)
    plan = scan.plan()
    expected_id = 1 if follow_up else 2
    assert plan.snapshot_id() == expected_id
    assert _rows(builder, plan) == [(i, (100 if follow_up else 200) + i) for i in range(32)]
    assert all(split.is_streaming() == follow_up for split in plan.splits())
    assert scan.checkpoint() == expected_id + 1
    if follow_up:
        assert scan.plan().snapshot_id() == 2
    assert scan.plan() is None
    assert scan.checkpoint() == 3
    scan.restore(1)
    assert scan.plan().snapshot_id() == 1
    scan.restore(None)
    assert scan.plan().snapshot_id() == 2


@pytest.mark.parametrize('follow_up', [False, True])
def test_bucket_callback_replacement_clearing_and_shard(table, follow_up):
    builder = table.new_read_builder()
    scan = builder.new_stream_scan().with_bucket_filter(lambda bucket: False)
    scan.restore(2 if follow_up else None)
    plan = scan.plan()
    assert plan is None if follow_up else plan.splits() == []
    scan.with_bucket_filter(lambda bucket: bucket % 2 == 0)
    scan.restore(2 if follow_up else None)
    selected_rows = _rows(builder, scan.plan())
    assert 0 < len(selected_rows) < 32
    shard = builder.new_stream_scan().with_shard(0, 2)
    shard.restore(2 if follow_up else None)
    assert selected_rows == _rows(builder, shard.plan())
    scan.with_bucket_filter(None)
    scan.restore(2 if follow_up else None)
    assert len(_rows(builder, scan.plan())) == 32
    union = []
    for index in range(2):
        shard = builder.new_stream_scan().with_shard(index, 2)
        shard.restore(2 if follow_up else None)
        union.extend(_rows(builder, shard.plan()))
    assert sorted(union) == [(i, 200 + i) for i in range(32)]


@pytest.mark.parametrize('follow_up', [False, True])
def test_callback_exception_preserves_identity_and_checkpoint(table, follow_up):
    calls = []
    failure = LookupError('bucket callback failed')

    def select(bucket):
        calls.append(bucket)
        if len(calls) == 2:
            raise failure
        return True

    scan = table.new_stream_scan().with_bucket_filter(select)
    scan.restore(1 if follow_up else None)
    with pytest.raises(LookupError) as caught:
        scan.plan()
    assert caught.value is failure
    assert len(calls) == 2
    assert scan.checkpoint() == (1 if follow_up else None)
    assert scan.plan().snapshot_id() == (1 if follow_up else 2)


@pytest.mark.parametrize('follow_up', [False, True])
def test_projection_and_predicate_come_from_read_builder(table, follow_up):
    builder = (table.new_read_builder().with_projection(['value']).with_filter({
        'method': 'equal', 'field': 'id', 'literals': [2]}))
    scan = builder.new_stream_scan()
    scan.restore(1 if follow_up else None)
    plan = scan.plan()
    assert [row for batch in builder.new_read().read(plan.splits()) for row in batch.to_pylist()] == [
        {'value': 102 if follow_up else 202}]


def test_consumer_is_only_persisted_by_explicit_acknowledgement(table):
    first = table.new_stream_scan().with_consumer_id('job')
    first.restore(1)
    assert first.plan().snapshot_id() == 1
    # Planning alone does not persist a consumer.
    unacknowledged = table.new_stream_scan().with_consumer_id('job')
    assert unacknowledged.plan().snapshot_id() == 2
    assert not unacknowledged.plan()
    first.notify_checkpoint_complete(first.checkpoint())
    resumed = table.new_stream_scan().with_consumer_id('job')
    plan = resumed.plan()
    assert plan.snapshot_id() == 2
    assert all(split.is_streaming() for split in plan.splits())
    assert resumed.checkpoint() == 3


def test_missing_checkpoint_waits_then_rejects_out_of_range(table):
    scan = table.new_read_builder().with_limit(0).new_stream_scan()
    scan.restore(12345)
    for _ in range(15):
        assert scan.plan() is None
        assert scan.checkpoint() == 12345
    with pytest.raises(ValueError, match='outside the available snapshots'):
        scan.plan()
    assert scan.checkpoint() == 12345


def test_invalid_configuration_keeps_the_scan_usable(table):
    scan = table.new_stream_scan()
    for value in [0, -1]:
        with pytest.raises(ValueError, match='snapshot id'):
            scan.restore(value)
    for index, count in [(0, 0), (2, 2)]:
        with pytest.raises(ValueError, match='shard'):
            scan.with_shard(index, count)
    with pytest.raises(TypeError, match='callable'):
        scan.with_bucket_filter(42)
    for value in ['', '.', '..', 'x/../../snapshot/snapshot-1', r'x\y']:
        with pytest.raises(ValueError, match='consumer id'):
            scan.with_consumer_id(value)
    assert scan.plan().snapshot_id() == 2


def test_snapshot_reader_is_an_internal_component():
    from pypaimon_rust import datafusion
    assert not hasattr(datafusion, 'SnapshotReader')
    assert not hasattr(datafusion.Table, 'new_snapshot_reader')
    assert not hasattr(datafusion.ReadBuilder, 'new_snapshot_reader')
