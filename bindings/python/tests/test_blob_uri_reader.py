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
