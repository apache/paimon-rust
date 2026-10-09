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


@pytest.fixture
def table(tmp_path):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE SCHEMA paimon.db")
    ctx.sql("CREATE TABLE paimon.db.t (id INT, embedding FLOAT[], pt INT) "
            "PARTITIONED BY (pt) WITH ('bucket'='-1', 'data-evolution.enabled'='true', "
            "'row-tracking.enabled'='true', 'global-index.enabled'='true', "
            "'vector-index.search-mode'='full')")
    table = PaimonCatalog({"warehouse": str(tmp_path)}).get_table("db.t")
    _write(table, [1, 2, 3, 4], [[1, 0], [0, 1], [1, 0], None], [0, 0, 1, 1])
    return table


def _write(table, ids, vectors, partitions):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.write_arrow(pa.record_batch([ids, vectors, partitions], schema=pa.schema([
            ('id', pa.int32()), ('embedding', pa.list_(pa.float32())), ('pt', pa.int32())])))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _single(table):
    return table.new_vector_search_builder().with_vector_column('embedding').with_query_vector([1, 0]).with_limit(10)


def test_filters_accumulate_and_partition_uses_table_schema(table):
    result = (_single(table).with_filter({'method': 'greaterThan', 'field': 'id', 'literals': [1]})
              .with_filter({'method': 'lessThan', 'field': 'id', 'literals': [4]})
              .with_partition_filter({'method': 'equal', 'field': 'pt', 'literals': [1]})
              .execute_local())
    assert len(result) == 1
    assert list(result.row_ids().values()) == [1.0]
    assert result.snapshot_id() == 1
    with pytest.raises(Exception, match='global row IDs'):
        result.positions()
    with pytest.raises(Exception, match='primary-key indexed splits'):
        result.splits()


@pytest.mark.parametrize('batch', [False, True])
def test_partition_filter_rejects_data_fields(table, batch):
    builder = table.new_batch_vector_search_builder() if batch else table.new_vector_search_builder()
    with pytest.raises(Exception, match='Partition filter'):
        builder.with_partition_filter({'method': 'equal', 'field': 'id', 'literals': [1]})
    with pytest.raises(ValueError, match='missing'):
        builder.with_filter({'method': 'equal', 'field': 'missing', 'literals': [1]})


def test_scan_read_keeps_snapshot_and_is_reusable(table):
    builder = _single(table)
    plan = builder.new_vector_search_scan().scan()
    _write(table, [5], [[1, 0]], [0])
    reader = builder.new_vector_search_read()
    result = reader.read_plan(plan)
    assert len(result) == 3
    rows = pa.Table.from_batches(result.new_read_builder().with_projection(['id']).read())
    assert rows.column_names == ['id', '__paimon_search_score']
    ids = rows.column('id').to_pylist()
    # Partition file publication determines global row IDs, so tied scores
    # may order IDs 1 and 3 either way. Both must precede the weaker hit.
    assert sorted(ids[:2]) == [1, 3]
    assert ids[2] == 2
    assert rows.column('__paimon_search_score').to_pylist() == pytest.approx([1, 1, 1 / 3])
    assert result.snapshot_id() == plan.snapshot_id() == 1
    assert reader.read_plan(plan).row_ids() == result.row_ids()
    assert len(builder.execute_local()) == 4
    filtered = _single(table).with_filter({'method': 'equal', 'field': 'id', 'literals': [1]})
    with pytest.raises(Exception, match='plan.*reader|context|different'):
        filtered.new_vector_search_read().read_plan(plan)


def test_batch_order_empty_results_and_scan_reuse(table):
    queries = [[1, 0], [0, 1], [1, 0]]
    builder = (table.new_batch_vector_search_builder().with_vector_column('embedding')
               .with_query_vectors(queries).with_limit(1))
    plan = builder.new_vector_search_scan().scan()
    results = builder.new_batch_vector_search_read().read_batch_plan(plan)
    assert [r.row_ids() for r in results] == [
        _single(table).with_query_vector(q).with_limit(1).execute_local().row_ids() for q in queries]
    assert [r.row_ids() for r in builder.execute_batch_local()] == [r.row_ids() for r in results]
    builder.with_filter({'method': 'equal', 'field': 'id', 'literals': [99]})
    assert [len(r) for r in builder.execute_batch_local()] == [0, 0, 0]


@pytest.mark.parametrize('query', [None, []])
def test_invalid_query_is_reported_before_planning(table, query):
    builder = table.new_vector_search_builder().with_vector_column('embedding').with_limit(1)
    if query is not None:
        builder.with_query_vector(query)
    with pytest.raises(Exception, match='vector|Vector'):
        builder.new_vector_search_read()
    with pytest.raises(Exception, match='Query vectors'):
        table.new_batch_vector_search_builder().with_vector_column('embedding').with_limit(1).with_query_vectors([]).execute_batch_local()
