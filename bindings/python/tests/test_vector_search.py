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

from pypaimon_rust.datafusion import GlobalIndexResult, PaimonCatalog, SQLContext


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


def test_vector_build_accepts_raw_schema_options_and_field_json(table):
    options = {
        'bucket': '-1', 'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'global-index.enabled': 'true', 'vector-index.search-mode': 'full',
        'global-index.ivf-flat.index-columns': 'embedding',
        'fields.embedding.pk-vector.index.options': 'invalid replaced JSON',
        'fields.embedding.index-options': '{"dimension":2,"nlist":1,"metric":"cosine"}',
        'ivf-flat.metric': 'l2', 'opaque.future.option': 'preserved',
    }
    configured = table.copy(options)
    builder = (configured.new_global_index_build_builder().with_index_column('embedding')
               .with_index_type('ivf-flat').with_options(options))
    assert builder.plan().shards()
    assert builder.execute() > 0
    actual = _single(configured).execute().row_ids()
    assert sorted(actual.values()) == pytest.approx([0, 1, 1])


@pytest.mark.parametrize('shared_json', [False, True, 'aliases'])
def test_full_text_json_encoded_lists_keep_raw_and_indexed_semantics(tmp_path, shared_json):
    import json
    ctx = SQLContext()
    ctx.register_catalog('paimon', {'warehouse': str(tmp_path)})
    ctx.sql('CREATE SCHEMA paimon.db')
    ctx.sql("CREATE TABLE paimon.db.texts (id INT, body STRING) WITH ("
            "'bucket'='-1','row-tracking.enabled'='true','data-evolution.enabled'='true',"
            "'global-index.enabled'='true','write-only'='true','full-text-index.search-mode'='full')")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('db.texts')
    write = table.new_batch_write_builder()
    writer, commit = write.new_write(), write.new_commit()
    try:
        writer.write_arrow(pa.record_batch([[1, 2], ['paimon data', 'needle']], schema=pa.schema([
            ('id', pa.int32()), ('body', pa.string())])))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    options = {'global-index.full-text.index-columns': 'body'}
    if shared_json == 'aliases':
        options.update({'full-text.stop-words': json.dumps(['paimon']), 'full-text.columns': json.dumps(['body'])})
    elif shared_json:
        options['fields.body.index-options'] = json.dumps({'stop-words': ['paimon'], 'text-fields': ['body']})
    else:
        options.update({'full-text.stop-words': json.dumps(['paimon']),
                        'full-text.text-fields': json.dumps(['body'])})
    table = table.copy(options)

    def hits(query):
        return (table.new_full_text_search_builder().with_query('body', json.dumps({'match': {'query': query}}))
                .with_limit(3).execute().row_ids())

    assert len(hits('paimon')) == 0
    before = hits('data')
    assert len(before) == 1
    builder = (table.new_global_index_build_builder().with_index_column('body')
               .with_index_type('full-text').with_options(options))
    assert builder.execute() == 1
    assert len(hits('paimon')) == 0
    assert hits('data') == before


def test_filters_accumulate_and_partition_uses_table_schema(table):
    result = (_single(table).with_filter({'method': 'greaterThan', 'field': 'id', 'literals': [1]})
              .with_filter({'method': 'lessThan', 'field': 'id', 'literals': [4]})
              .with_partition_filter({'method': 'equal', 'field': 'pt', 'literals': [1]})
              .execute())
    assert isinstance(result, GlobalIndexResult)
    assert len(result) == 1
    assert list(result.row_ids().values()) == [1.0]
    assert result.snapshot_id() == 1
    with pytest.raises(Exception, match='global row IDs'):
        result.positions()
    with pytest.raises(Exception, match='primary-key indexed splits'):
        result.splits()


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('batch', [False, True])
def test_indexed_partition_filter_uses_table_field_positions(table, mode, batch):
    (table.new_global_index_build_builder().with_index_column('embedding')
     .with_index_type('ivf-flat').with_options({
         'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1',
         'ivf-flat.distance.metric': 'l2'}).execute())
    (table.new_global_index_build_builder().with_index_column('id')
     .with_index_type('btree').execute())
    table = table.copy({'vector-index.search-mode': mode,
                        'global-index.filter.refine-from-data': 'false'})
    builder = (table.new_batch_vector_search_builder().with_vector_column('embedding')
               .with_query_vectors([[1, 0], [1, 0]]).with_limit(3) if batch else _single(table))
    # pt is field 2 in the table and field 0 in the partition row. An exact
    # scalar lookup must retain the matching indexed vector in either API.
    builder.with_filter({'method': 'equal', 'field': 'id', 'literals': [3]})
    builder.with_partition_filter({'method': 'equal', 'field': 'pt', 'literals': [1]})
    results = builder.execute() if batch else [builder.execute()]
    for result in results:
        assert len(result) == 1
        assert list(result.row_ids().values()) == [1.0]
        rows = pa.Table.from_batches(result.new_read_builder().with_projection(['id']).read())
        assert rows.column('id').to_pylist() == [3]


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
    assert result.snapshot_id() == plan.snapshot().id() == 1
    assert reader.read_plan(plan).row_ids() == result.row_ids()
    assert len(builder.execute()) == 4
    filtered = _single(table).with_filter({'method': 'equal', 'field': 'id', 'literals': [1]})
    selected = filtered.new_vector_search_read().read_plan(plan)
    assert len(selected) == 1
    assert selected.snapshot_id() == 1
    rows = pa.Table.from_batches(selected.new_read_builder().with_projection(['id']).read())
    assert rows.column('id').to_pylist() == [1]


def test_batch_order_empty_results_and_scan_reuse(table):
    queries = [[1, 0], [0, 1], [1, 0]]
    builder = (table.new_batch_vector_search_builder().with_vector_column('embedding')
               .with_query_vectors(queries).with_limit(1))
    plan = builder.new_vector_search_scan().scan()
    results = builder.new_batch_vector_search_read().read_batch_plan(plan)
    assert all(isinstance(r, GlobalIndexResult) for r in results)
    assert [r.row_ids() for r in results] == [
        _single(table).with_query_vector(q).with_limit(1).execute().row_ids() for q in queries]
    assert [r.row_ids() for r in builder.execute()] == [r.row_ids() for r in results]
    builder.with_filter({'method': 'equal', 'field': 'id', 'literals': [99]})
    assert [len(r) for r in builder.execute()] == [0, 0, 0]


@pytest.mark.parametrize('query', [None, []])
def test_invalid_query_is_reported_before_planning(table, query):
    builder = table.new_vector_search_builder().with_vector_column('embedding').with_limit(1)
    if query is not None:
        builder.with_query_vector(query)
    with pytest.raises(Exception, match='vector|Vector'):
        builder.new_vector_search_read()
    with pytest.raises(Exception, match='Query vectors'):
        (table.new_batch_vector_search_builder().with_vector_column('embedding')
         .with_limit(1).with_query_vectors([]).execute())


def test_single_vector_keyword_matches_stub(table):
    builder = _single(table).with_query_vector(vector=[1, 0]).with_limit(1)
    assert builder.execute().row_ids() == _single(table).with_limit(1).execute().row_ids()


@pytest.mark.parametrize('batch', [False, True])
def test_raw_options_survive_scan_read_and_use_raw_metric_options(table, batch):
    import pickle
    builder = (table.new_batch_vector_search_builder().with_query_vectors([[1, 0], [1, 0]])
               if batch else table.new_vector_search_builder().with_query_vector([1, 0]))
    builder.with_options({'global-index.ivf-flat.index-columns': 'embedding',
                          'fields.embedding.index-options': '{"dimension":2,"nlist":1}',
                          'opaque.future.option': '{"value":[1,2]}'})
    builder.with_vector_column('embedding').with_limit(3)
    plan = pickle.loads(pickle.dumps(builder.new_vector_search_scan().scan()))
    reader = builder.new_batch_vector_search_read() if batch else builder.new_vector_search_read()
    # Mutating the builder must not change captured reader configuration.
    builder.with_options({'global-index.ivf-flat.index-columns': 'other',
                          'global-index.diskann.index-columns': 'embedding', 'metric': 'l2'})
    results = reader.read_batch_plan(plan) if batch else [reader.read_plan(plan)]
    for result in results:
        assert sorted(result.row_ids().values()) == pytest.approx([0, 1, 1])
    results = builder.execute() if batch else [builder.execute()]
    for result in results:
        assert sorted(result.row_ids().values()) == pytest.approx([1 / 3, 1, 1])


def test_options_keep_index_metadata_and_ignore_removed_option_hints(table):
    expected = _single(table).execute().row_ids()
    for key in ('index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'):
        assert _single(table.copy({key: 'diskann'})).with_option(key, 'ivf-flat').execute().row_ids() == expected
    (table.new_global_index_build_builder().with_index_column('embedding')
     .with_index_type('ivf-flat').with_options({'dimension': '2', 'nlist': '1', 'metric': 'l2'}).execute())
    expected = _single(table).execute().row_ids()
    assert (_single(table).with_options({'global-index.invalid-unused-type.index-columns': 'embedding'})
            .execute().row_ids()) == expected
    # Table defaults are not explicit query metrics. Both JSON namespaces obey
    # that boundary when the persisted index uses a different metric.
    for key, value in (
        ('ivf-flat.metric', 'cosine'),
        ('fields.embedding.index-options', '{"metric":"cosine"}'),
        ('fields.embedding.pk-vector.index.options', '{"metric":"cosine"}'),
    ):
        assert (_single(table.copy({key: value, 'global-index.diskann.index-columns': 'embedding'}))
                .execute().row_ids()) == expected
        with pytest.raises(Exception, match='does not match index metric'):
            _single(table).with_option(key, value).execute()


def test_hybrid_options_are_captured_with_the_route_builder(table):
    hybrid = table.new_hybrid_search_builder()
    hybrid.add_vector_route('embedding', [1, 0], 3, options={
        'global-index.ivf-flat.index-columns': 'embedding', 'ivf-flat.metric': 'l2'}).with_limit(3)
    captured = hybrid.route_builders()[0].search_builder()
    hybrid.add_vector_route('embedding', [1, 0], 3, options={
        'global-index.diskann.index-columns': 'embedding', 'diskann.metric': 'inner_product'})
    assert sorted(captured.execute().row_ids().values()) == pytest.approx([1 / 3, 1, 1])
    updated = hybrid.route_builders()[1].search_builder()
    assert sorted(updated.execute().row_ids().values()) == pytest.approx([0, 1, 1])


def test_builders_have_no_separate_type_setter(table):
    for builder in (table.new_vector_search_builder(), table.new_batch_vector_search_builder(),
                    table.new_hybrid_search_builder()):
        assert not hasattr(builder, 'with_vector_index_type')


def test_external_result_uses_same_scan_type_without_search_context(table):
    found = _single(table).with_limit(1).execute()
    external = GlobalIndexResult.from_scored_row_ids(found.row_ids())
    builder = table.new_read_builder().with_projection(['id'])
    plan = builder.new_scan().with_global_index_result(external).plan()
    expected = pa.Table.from_batches(found.new_read_builder().with_projection(['id']).read())
    actual = pa.Table.from_batches(builder.new_read().read(plan.splits()))
    assert actual.column('id').to_pylist() == expected.column('id').to_pylist()
    assert external.row_ids() == found.row_ids()
    with pytest.raises(Exception, match='require a result returned by a table search'):
        external.new_read_builder().read()
    with pytest.raises(TypeError):
        builder.new_scan().with_global_index_result(found.row_ids())
