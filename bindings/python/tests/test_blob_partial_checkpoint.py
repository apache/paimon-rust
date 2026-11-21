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

from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon_rust.data import VideoFrameDescriptor
from pypaimon_rust.datafusion import PaimonCatalog, SQLContext


@pytest.mark.parametrize('video', [False, True], ids=['blob', 'video'])
def test_update_checkpoints_then_append_without_blob_preserves_payloads(tmp_path, video):
    source = tmp_path / 'source.mp4'
    source.write_bytes(b'abc')
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path)})
    context.sql('CREATE SCHEMA paimon.checkpoints')
    video_option = ", 'video-frame-field' = 'frame'" if video else ''
    context.sql("""CREATE TABLE paimon.checkpoints.t (id INT, n INT, frame BLOB) WITH (
        'data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true',
        'compaction.min.file-num' = '5' %s)"""
                % video_option)
    table = PaimonCatalog({'warehouse': str(tmp_path)}).get_table('checkpoints.t')
    payload = (VideoFrameDescriptor(source.as_uri(), 0, 3, 7).serialize()
               if video else b'abc')

    def batch(index):
        return pa.record_batch([pa.array([index], type=pa.int32()),
                                pa.array([index], type=pa.int32()),
                                pa.array([payload], type=pa.large_binary())],
                               names=['id', 'n', 'frame'])

    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(batch(0))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()
    row_id = context.sql('SELECT "_ROW_ID" FROM paimon.checkpoints.t')[0].column(0)[0].as_py()
    builder = table.new_batch_write_builder()
    update = builder.new_update()
    builder.new_commit().commit(update.update_by_arrow_with_row_id(pa.table({
        '_ROW_ID': pa.array([row_id], type=pa.int64()),
        'n': pa.array([99], type=pa.int32()),
    })))

    builder = table.new_stream_write_builder().with_commit_user('blob-checkpoints')
    writer = builder.new_write()
    try:
        for checkpoint in [1, 2]:
            writer.write_arrow(batch(checkpoint))
            builder.new_commit().commit(checkpoint, writer.prepare_commit(True, checkpoint))
    finally:
        writer.close()

    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.with_write_type(['id', 'n'])
        writer.write_arrow(pa.record_batch([pa.array([4], type=pa.int32()),
                                           pa.array([4], type=pa.int32())],
                                          names=['id', 'n']))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()

    # The seed, update and three appends reach five normal files. Compaction merges
    # their logical rows, while the physical BLOB files cover only the first three.
    files = pa.Table.from_batches(context.sql(
        'SELECT file_format, record_count, first_row_id, file_source '
        'FROM paimon.checkpoints.t$files')).to_pylist()
    assert any(file['file_format'] == 'parquet' and file['record_count'] == 4
               and file['first_row_id'] == row_id and file['file_source'] == 'COMPACT'
               for file in files)
    rows = pa.Table.from_batches(context.sql(
        'SELECT "_ROW_ID", id, n, frame FROM paimon.checkpoints.t ORDER BY id')).to_pydict()
    assert rows['id'] == [0, 1, 2, 4]
    assert rows['n'] == [99, 1, 2, 4]
    assert rows['_ROW_ID'] == [row_id + offset for offset in range(4)]
    assert rows['frame'][3] is None
    if video:
        for value in rows['frame'][:3]:
            frame = VideoFrameDescriptor.deserialize(value)
            assert frame.frame_index == 7
            assert frame.length == 3
            assert frame.uri.endswith('.video')
            uri = urlparse(frame.uri)
            path = Path(uri.path if uri.scheme == 'file' else frame.uri)
            with path.open('rb') as packed:
                packed.seek(frame.offset)
                assert packed.read(frame.length) == b'abc'
    else:
        assert rows['frame'][:3] == [b'abc'] * 3
    omitted = pa.Table.from_batches(context.sql(
        'SELECT id, frame FROM paimon.checkpoints.t WHERE "_ROW_ID" = %d'
        % (row_id + 3))).to_pydict()
    assert omitted == {'id': [4], 'frame': [None]}
