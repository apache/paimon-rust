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

import io
import struct

import pyarrow as pa
import pytest

from test_blob_consumer import _table
from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def descriptor(offset, length):
    uri = b'custom://source'
    return struct.pack('<BQi', 2, 0x424C4F4244455343, len(uri)) + uri + struct.pack('<qq', offset, length)


class ReaderFactory:
    def __init__(self, no_seek=False):
        self.no_seek = no_seek
        self.opens = self.closes = 0
        self.failure = None

    def create(self, uri):
        assert uri == 'custom://source'
        return self

    def new_input_stream(self, uri):
        self.opens += 1
        owner = self

        class Stream(io.BytesIO):
            def read(self, length=-1):
                if owner.failure:
                    raise owner.failure
                return super().read(min(length, 2))

            def seek(self, offset, whence=0):
                if owner.no_seek:
                    raise io.UnsupportedOperation('cannot seek')
                return super().seek(offset, whence)

            def close(self):
                if not self.closed:
                    owner.closes += 1
                super().close()

        return Stream(b'0123456789')


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('no_seek', [False, True])
def test_custom_uri_streams_are_core_bounded_and_rewind_failures_recover(tmp_path, stream, no_seek):
    context, table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    factory = ReaderFactory(no_seek)
    try:
        assert writer.with_blob_uri_reader_factory(factory) is writer
        data = pa.record_batch([pa.array([1, 2, 3], type=pa.int32()),
                                pa.array([descriptor(0, 3), descriptor(3, 2), descriptor(0, 3)],
                                         type=pa.large_binary())], names=['id', 'payload'])
        writer.write_arrow(data)
        with pytest.raises(ValueError, match='before any write'):
            writer.with_blob_uri_reader_factory(None)
        messages = writer.prepare_commit(True, 1) if stream else writer.prepare_commit()
        commit = builder.new_commit()
        commit.commit(1, messages) if stream else commit.commit(messages)
        assert pa.Table.from_batches(context.sql(
            'SELECT id, payload FROM paimon.consumer.t ORDER BY id')).to_pydict() == {
                'id': [1, 2, 3], 'payload': [b'012', b'34', b'012']}
        assert factory.opens == factory.closes == (2 if no_seek else 1)
    finally:
        writer.close()


def test_custom_uri_read_error_preserves_original_exception_and_closes_once(tmp_path):
    _, table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    factory = ReaderFactory()
    factory.failure = LookupError('opaque source failed')
    try:
        writer.with_blob_uri_reader_factory(factory)
        data = pa.record_batch([pa.array([1], type=pa.int32()),
                                pa.array([descriptor(0, 3)], type=pa.large_binary())],
                               names=['id', 'payload'])
        with pytest.raises(LookupError) as caught:
            writer.write_arrow(data)
        assert caught.value is factory.failure
        assert factory.opens == factory.closes == 1
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.blob'))


def _update_table(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.blobs')
    context.sql("""CREATE TABLE paimon.blobs.t (id INT, payload BLOB) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'blob.copy-buffer-size' = '2 B')""")
    return context, PaimonCatalog({'warehouse': str(tmp_path)}).get_table('blobs.t')


def _reference(uri, offset=0, length=-1):
    encoded = uri.encode('utf-8')
    return (struct.pack('<BQi', 2, 0x424C4F4244455343, len(encoded)) + encoded
            + struct.pack('<qq', offset, length))


def _batch(ids, values):
    return pa.record_batch([pa.array(ids, type=pa.int32()), pa.array(values, type=pa.large_binary())],
                           names=['id', 'payload'])


def _read(context):
    return pa.Table.from_batches(context.sql('SELECT id, payload FROM paimon.blobs.t ORDER BY id')).to_pydict()


@pytest.mark.parametrize('stream', [False, True])
def test_scoped_blob_factory_keeps_native_file_references(tmp_path, stream):
    context, table = _update_table(tmp_path)
    source = tmp_path / 'source'
    source.write_bytes(b'prefixREFERENCEDsuffix')
    streams = []
    selected = []

    class Reader:
        def new_input_stream(self, uri):
            assert uri == 'custom://object'
            value = io.BytesIO(b'OBJECT')
            streams.append(value)
            return value

    class Factory:
        def _supports_uri(self, uri):
            selected.append(uri)
            return uri.startswith('custom://')

        def create(self, uri):
            assert uri == 'custom://object', 'Native references must not be delegated to Python'
            return Reader()

    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.with_blob_uri_reader_factory(Factory())
        writer.write_arrow(_batch([0, 1], [_reference('custom://object'), _reference(source.as_uri(), 6, 10)]))
        assert len(streams) == 1 and streams[0].closed
        messages = writer.prepare_commit(True, 7) if stream else writer.prepare_commit()
        commit = builder.new_commit()
        commit.commit(7, messages) if stream else commit.commit(messages)
        assert selected == ['custom://object', source.as_uri()]
        assert _read(context) == {'id': [0, 1], 'payload': [b'OBJECT', b'REFERENCED']}
    finally:
        writer.close()


@pytest.mark.parametrize('stream, operation', [
    (False, 'merge'), (True, 'merge'), (False, 'row_id'), (True, 'row_id'),
    (False, 'grouped_row_id'), (False, 'incremental'), (True, 'incremental'),
    (False, 'upsert'), (True, 'upsert'),
])
@pytest.mark.parametrize('failure_stage', ['select', 'create'])
def test_update_factory_errors_keep_identity_and_existing_data(tmp_path, stream, operation, failure_stage):
    context, table = _update_table(tmp_path)
    seed_builder = table.new_batch_write_builder()
    seed = seed_builder.new_write()
    try:
        seed.write_arrow(_batch([0], [b'old']))
        seed_builder.new_commit().commit(seed.prepare_commit())
    finally:
        seed.close()
    row_id = context.sql('SELECT "_ROW_ID" FROM paimon.blobs.t')[0].column(0)[0].as_py()
    error = OSError('original factory failure')
    calls = []
    source_file = tmp_path / 'source'
    source_file.write_bytes(b'native-readable')

    class Factory:
        def _supports_uri(self, uri):
            calls.append('select')
            if failure_stage == 'select':
                raise error
            return True

        def create(self, uri):
            calls.append('create')
            if failure_stage == 'select':
                pytest.fail('Selection errors must not fall back to any reader')
            raise error

    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    update._with_blob_uri_reader_factory(Factory())
    reference = _reference(source_file.as_uri())
    source = pa.Table.from_batches([_batch([0], [reference])])
    data = pa.table({'_ROW_ID': [row_id], 'payload': pa.array([reference], type=pa.large_binary())})
    extra = {'commit_identifier': 7} if stream else {}
    with pytest.raises(OSError) as caught:
        if operation == 'merge':
            update.merge_into(source, on=[('id', 'id')], when_matched=[{
                'delete': False, 'assignments': [('payload', 'source', 'payload')],
            }], when_not_matched=[], **extra)
        elif operation == 'row_id':
            update.update_by_arrow_with_row_id(data, **extra)
        elif operation == 'grouped_row_id':
            update.update_by_arrow_batches_with_row_id([data], **extra)
        elif operation == 'incremental':
            update.new_update_by_row_id(**extra).update_columns(data, ['payload'])
        else:
            update.with_update_type(['payload']).upsert_by_arrow_with_key(source, ['id'], **extra)
    assert caught.value is error
    assert calls == (['select'] if failure_stage == 'select' else ['select', 'create'])
    assert _read(context) == {'id': [0], 'payload': [b'old']}
