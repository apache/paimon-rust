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

import struct
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


def _table(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.consumer')
    context.sql("""CREATE TABLE paimon.consumer.t (id INT, payload BLOB) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
        'blob.copy-buffer-size' = '2 B')""")
    return context, PaimonCatalog({'warehouse': str(tmp_path)}).get_table('consumer.t')


def _batch():
    return pa.record_batch([pa.array([1, 2, 3], type=pa.int32()),
                            pa.array([b'payload', None, b''], type=pa.large_binary())],
                           names=['id', 'payload'])


def _descriptor_payload(encoded):
    version, magic, length = struct.unpack_from('<BQi', encoded)
    assert (version, magic) == (2, 0x424C4F4244455343)
    uri = encoded[13:13 + length].decode('utf-8')
    offset, size = struct.unpack_from('<qq', encoded, 13 + length)
    assert uri.endswith('.blob') and size >= 0
    path = urlparse(uri).path if uri.startswith('file:') else uri
    with open(path, 'rb') as source:
        source.seek(offset)
        return source.read(size)


@pytest.mark.parametrize('stream', [False, True])
def test_blob_consumer_descriptor_bytes_and_nulls(tmp_path, stream):
    context, table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    received = []

    def callback(name, encoded):
        received.append((name, encoded))
        return True

    try:
        assert writer.with_blob_consumer(callback) is writer
        writer.write_arrow(_batch())
        with pytest.raises(ValueError, match='before any write'):
            writer.with_blob_consumer(None)
        messages = writer.prepare_commit(True, 1) if stream else writer.prepare_commit()
        commit = builder.new_commit()
        commit.commit(1, messages) if stream else commit.commit(messages)
        assert [name for name, _ in received] == ['payload'] * 3
        assert [_descriptor_payload(encoded) if encoded is not None else None
                for _, encoded in received] == [b'payload', None, b'']
        assert pa.Table.from_batches(context.sql(
            'SELECT id, payload FROM paimon.consumer.t ORDER BY id')).to_pydict() == {
            'id': [1, 2, 3], 'payload': [b'payload', None, b'']}
    finally:
        writer.close()
    with pytest.raises(RuntimeError, match='closed'):
        writer.with_blob_consumer(None)


@pytest.mark.parametrize('stream', [False, True])
def test_blob_consumer_exception_is_not_wrapped_or_retried(tmp_path, stream):
    _, table = _table(tmp_path)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    calls = []
    failure = KeyError('callback failure')

    def callback(name, encoded):
        calls.append((name, encoded))
        raise failure

    try:
        writer.with_blob_consumer(callback)
        with pytest.raises(KeyError) as caught:
            writer.write_arrow(_batch())
        assert caught.value is failure
        assert [name for name, _ in calls] == ['payload']
        assert list(tmp_path.rglob('*.blob'))
        assert _descriptor_payload(calls[0][1]) == b'payload'
        assert not list(tmp_path.rglob('*.parquet'))
        with pytest.raises(ValueError, match='write failure'):
            writer.write_arrow(_batch())
        assert [name for name, _ in calls] == ['payload']
    finally:
        writer.close()


def test_blob_consumer_replacement_and_clear(tmp_path):
    _, table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_consumer(lambda name, encoded: pytest.fail('cleared callback invoked'))
        with pytest.raises(TypeError, match='callable'):
            writer.with_blob_consumer(123)
        writer.with_blob_consumer(None)
        writer.write_arrow(_batch())
    finally:
        writer.close()
