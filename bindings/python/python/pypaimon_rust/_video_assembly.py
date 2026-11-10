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

"""Assemble ordered query results into a single MP4."""

from contextlib import closing
from fractions import Fraction
from itertools import chain, groupby
from operator import itemgetter

from ._video_index import VideoSeekMismatch, read_moov
from ._video_remux import remux_video_range
from .functions import (
    _encode_mp4,
    _encode_video_range,
    _video_range_frames,
    _video_source,
    open_blob_descriptor_stream,
)


def _media_rows(reader, column_index, registry):
    values = (value for batch in reader for value in batch.column(column_index))
    for row, value in enumerate(values, 1):
        raw = value.as_py()
        if raw is None:
            raise ValueError(f"read_video row {row} contains NULL")
        descriptor = registry.describe_video_frame(raw)
        if descriptor is None:
            yield None, row, raw, None
        else:
            payload, ordinal = descriptor
            yield (payload, ordinal - row), row, raw, ordinal


def _media_runs(reader, column_index, registry):
    # Consecutive rows from one payload have the same ordinal minus row number.
    for key, group in groupby(_media_rows(reader, column_index, registry), key=itemgetter(0)):
        if key is None:
            for _, row, raw, _ in group:
                yield row, raw, None
        else:
            _, row, raw, start = next(group)
            end = start + 1 + sum(1 for _ in group)
            yield row, raw, (start, end)


def _decode_video_run(av, raw, frame_range, registry):
    start, end = frame_range
    stream, moov_range = registry.open_video_frame_descriptor_stream(raw)
    with stream:
        moov = read_moov(stream, moov_range) if moov_range and start else None
        while start < end:
            try:
                with av.open(stream, mode="r") as container:
                    source = _video_source(container)
                    if source is None:
                        raise ValueError("Video frame descriptor has no video stream")
                    for frame in _video_range_frames(container, source, start, end, moov):
                        start += 1
                        yield frame
                return
            except VideoSeekMismatch:
                # Already yielded frames matched the index. Resume at the next ordinal.
                if moov is None:
                    raise
                stream.seek(0)
                moov = None


def _decode_media_runs(av, runs, registry):
    for row, raw, frame_range in runs:
        if frame_range is not None:
            yield from _decode_video_run(av, raw, frame_range, registry)
        else:
            try:
                from PIL import Image
            except ImportError as e:
                raise ImportError("Pillow is required to read image frames") from e
            with open_blob_descriptor_stream(raw, registry) as stream:
                try:
                    with Image.open(stream) as image:
                        yield av.VideoFrame.from_image(image)
                except (OSError, ValueError) as e:
                    raise ValueError(f"read_video row {row} is not a decodable image") from e


def read_video(reader, column, fps, registry):
    try:
        import pyarrow as pa

        index = reader.schema.get_field_index(column)
        if index < 0:
            raise ValueError(f"Query must return exactly one column named {column!r}")
        data_type = reader.schema.field(index).type
        if not (
            pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
            or pa.types.is_binary_view(data_type)
            or pa.types.is_fixed_size_binary(data_type)
        ):
            raise ValueError(f"read_video column {column!r} must be binary")

        runs = _media_runs(reader, index, registry)
        first = next(runs, None)
        if first is None:
            raise ValueError("read_video query returned no rows")
        if fps is None:
            _, raw, frame_range = first
            if frame_range is None or next(runs, None) is not None:
                raise ValueError("fps is required unless the query returns one continuous video range")
            stream, moov_range = registry.open_video_frame_descriptor_stream(raw)
            with stream:
                moov = read_moov(stream, moov_range) if moov_range else None
                if moov is not None:
                    clip = remux_video_range(stream, *frame_range, moov)
                    if clip is not None:
                        return clip
                    stream.seek(0)
                clip = _encode_video_range(stream, *frame_range, moov)
            if clip is None:
                raise ValueError("Could not decode the complete video range")
            return clip

        rate = Fraction(str(fps)).limit_denominator(100_000)
        if rate <= 0 or rate.numerator >= 2**31:
            raise ValueError("fps is outside the supported range")
        try:
            import av
        except ImportError as e:
            raise ImportError("PyAV is required to assemble MP4 videos") from e
        with closing(_decode_media_runs(av, chain((first,), runs), registry)) as frames:
            first_frame = next(frames)
            return _encode_mp4(
                chain((first_frame,), frames),
                first_frame.width,
                first_frame.height,
                rate,
                1 / rate,
                preserve_timing=False,
            )
    finally:
        reader.close()
