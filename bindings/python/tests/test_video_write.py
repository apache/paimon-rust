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

import gc
import io
import struct
import threading
import weakref
from pathlib import Path

import pyarrow as pa
import pytest

from pypaimon_rust.data import VideoFormatWriter, VideoFrameDescriptor
from pypaimon_rust.datafusion import PaimonCatalog, SQLContext
from test_blob_uri_reader import ReaderFactory


class Output(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.result = None
        self.close_count = 0

    def close(self):
        if not self.closed:
            self.result = self.getvalue()
            self.close_count += 1
        super().close()


def fixture(name):
    root = Path(__file__).resolve().parents[3]
    text = (root / 'crates/paimon/testdata/video' / name).read_text()
    return bytes.fromhex(next(line for line in text.splitlines()
                              if line and not line.startswith('#')))


def test_descriptor_matches_pypaimon_wire_layout_and_java_fixture():
    value = VideoFrameDescriptor('s3://bucket/视频.mp4', 7, 99, 42)
    assert value.serialize() == fixture('video-frame-descriptor-v1.hex')
    assert VideoFrameDescriptor.deserialize(value.serialize()) == value
    uri = b'custom://source'
    # PyPaimon serializes descriptors independently of the native binding.
    encoded = struct.pack('<BQi', 1, 0x564944454F46524D, len(uri)) + uri
    encoded += struct.pack('<qqqqq', 0, 3, 7, 0, 0)
    restored = VideoFrameDescriptor.deserialize(encoded)
    assert (restored.uri, restored.offset, restored.length, restored.frame_index) == (
        'custom://source', 0, 3, 7)
    assert restored.serialize() == encoded
    with pytest.raises(ValueError):
        VideoFrameDescriptor('x', -1, 3, 0)


def test_low_level_writer_matches_golden_file_and_owns_output(tmp_path):
    source = tmp_path / 'mp4'
    expected = fixture('video-v1-moov.hex')
    source.write_bytes(expected[:24])
    output = Output()
    writer = VideoFormatWriter(output, copy_buffer_size=1)
    writer.write_frames([VideoFrameDescriptor(source.as_uri(), 0, -1, 7).serialize()])
    writer.write_arrow(pa.record_batch([
        pa.array([VideoFrameDescriptor(source.as_uri(), 0, -1, 8).serialize()],
                 type=pa.large_binary())], names=['frame']))
    assert writer.num_bytes() == 24
    assert writer.close() == len(expected)
    assert writer.close() == len(expected)
    assert writer.num_bytes() == len(expected)
    assert output.result == expected
    assert output.close_count == 1
    with pytest.raises(RuntimeError, match='closed'):
        writer.write_frames([])


def test_pypaimon_descriptors_writer_and_reader_interoperate(tmp_path):
    blob = pytest.importorskip('pypaimon.table.row.blob')
    from pypaimon.execution.python.read.reader.video_format_reader import VideoFileMeta
    from pypaimon.execution.python.write.video_format_writer import VideoFormatWriter as PythonWriter
    from pypaimon.schema.data_types import AtomicType, DataField
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.table.row.row_kind import RowKind

    source = tmp_path / 'source.mp4'
    source.write_bytes(fixture('video-v1-moov.hex')[:24])

    class Reader:
        def new_input_stream(self, uri):
            assert uri == source.as_uri()
            return source.open('rb')

    reader = Reader()

    class Factory:
        def create(self, uri):
            return reader

    descriptors = [blob.VideoFrameDescriptor(source.as_uri(), 0, 24, index)
                   for index in [7, 8, 10]]
    encoded = [value.serialize() for value in descriptors] + [None]
    output = Output()
    writer = VideoFormatWriter(output, uri_reader_factory=Factory())
    writer.write_frames(encoded)
    writer.close()
    meta = VideoFileMeta(io.BytesIO(output.result), len(output.result))
    assert meta.record_count == 4
    assert [meta.frame(i) for i in range(4)] == [
        (0, 24, 7), (0, 24, 8), (0, 24, 10), None]
    python_output = Output()
    python_writer = PythonWriter(python_output)
    field = DataField(0, 'frame', AtomicType('BLOB'))
    for descriptor in [*descriptors, None]:
        value = None if descriptor is None else blob.Blob.from_descriptor(reader, descriptor)
        python_writer.add_element(GenericRow([value], [field], RowKind.INSERT))
    python_writer.close()
    assert output.result == python_output.result


def test_custom_sources_are_bounded_deduplicated_and_closed_once():
    factory = ReaderFactory()
    output = Output()
    writer = VideoFormatWriter(output, uri_reader_factory=factory, copy_buffer_size=3)
    value = VideoFrameDescriptor('custom://source', 2, 3, 7).serialize()
    writer.write_frames([value])
    writer.write_frames([value, None])
    writer.close()
    assert output.result.startswith(b'234')
    assert writer.num_bytes() == len(output.result)
    assert factory.opens == factory.closes == 1


def test_source_failure_preserves_python_exception_and_closes_output():
    factory = ReaderFactory()
    factory.failure = LookupError('video source failed')
    output = Output()
    writer = VideoFormatWriter(output, uri_reader_factory=factory)
    with pytest.raises(LookupError) as caught:
        writer.write_frames([VideoFrameDescriptor('custom://source', 0, 3, 0).serialize()])
    assert caught.value is factory.failure
    assert factory.opens == factory.closes == 1
    assert output.close_count == 1
    assert output.result == b''
    writer.abort()


@pytest.mark.parametrize('failure', ['write', 'flush', 'close', 'short'])
def test_destination_failure_closes_once_and_preserves_exception(failure):
    original = OSError('video output failed')

    class FailingOutput(Output):
        def write(self, value):
            if failure == 'write':
                raise original
            if failure == 'short':
                return max(0, len(value) - 1)
            return super().write(value)

        def flush(self):
            if failure == 'flush':
                raise original
            return super().flush()

        def close(self):
            super().close()
            if failure == 'close':
                raise original

    output = FailingOutput()
    writer = VideoFormatWriter(output)
    writer.write_frames([None])
    with pytest.raises(OSError) as caught:
        writer.close()
    if failure != 'short':
        assert caught.value is original
    assert output.close_count == 1
    writer.abort()


def test_abandoned_writer_cycles_are_collected_on_another_thread():
    output = Output()
    factory = ReaderFactory()
    writer = VideoFormatWriter(output, uri_reader_factory=factory)
    output.writer = factory.writer = writer
    references = [weakref.ref(output), weakref.ref(factory)]
    del writer, output, factory
    collector = threading.Thread(target=gc.collect)
    collector.start()
    collector.join()
    assert all(reference() is None for reference in references)


@pytest.mark.parametrize('stream', [False, True])
def test_table_writer_accepts_video_descriptors_and_reads_lazy_frames(tmp_path, stream):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.videos')
    context.sql("""CREATE TABLE paimon.videos.t (id INT, frame BLOB) WITH (
        'data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true',
        'video-frame-field' = 'frame', 'target-file-row-num' = '2')""")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('videos.t')
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    factory = ReaderFactory()
    frames = [VideoFrameDescriptor('custom://source', 0, 5, index).serialize()
              for index in [7, 8, 10]]
    try:
        with pytest.raises(NotImplementedError, match='BlobConsumer'):
            writer.with_blob_consumer(lambda *args: False)
        writer.with_blob_uri_reader_factory(factory)
        data = pa.record_batch([pa.array([0, 1, 2, 3], type=pa.int32()),
                                pa.array([*frames, None], type=pa.large_binary())],
                               names=['id', 'frame'])
        writer.write_arrow(data.slice(0, 1))
        writer.write_arrow(data.slice(1))
        messages = writer.prepare_commit(True, 1) if stream else writer.prepare_commit()
        committer = builder.new_commit()
        committer.commit(1, messages) if stream else committer.commit(messages)
    finally:
        writer.close()
    assert factory.opens == factory.closes == 2  # one copy per rolled file
    assert list(tmp_path.rglob('*.video'))
    assert not list(tmp_path.rglob('*.blob'))
    rows = pa.Table.from_batches(context.sql(
        'SELECT id, frame FROM paimon.videos.t ORDER BY id')).to_pydict()
    assert rows['id'] == [0, 1, 2, 3]
    assert rows['frame'][3] is None
    for encoded, index in zip(rows['frame'], [7, 8, 10]):
        frame = VideoFrameDescriptor.deserialize(encoded)
        assert frame.frame_index == index
        assert frame.uri.endswith('.video')
        assert frame.length == 5


def test_table_write_failure_discards_all_partial_files(tmp_path):
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.videos')
    context.sql("""CREATE TABLE paimon.videos.t (id INT, frame BLOB) WITH (
        'data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true',
        'video-frame-field' = 'frame')""")
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('videos.t')
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.record_batch([pa.array([0], type=pa.int32()),
                                           pa.array([b'not a frame descriptor'],
                                                    type=pa.large_binary())],
                                          names=['id', 'frame']))
        pytest.fail('raw video bytes must be rejected')
    except ValueError:
        pass
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.video'))
    assert not list(tmp_path.rglob('*.parquet'))
