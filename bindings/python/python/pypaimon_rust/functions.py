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
import logging
import struct
from typing import Any, BinaryIO

logger = logging.getLogger(__name__)
_STILL_IMAGE_FORMATS = {
    "apng",
    "bmp_pipe",
    "gif",
    "ico",
    "image2",
    "image2pipe",
    "jpeg_pipe",
    "png_pipe",
    "tiff_pipe",
    "webp_pipe",
}


class _BlobDescriptorProbe:
    CURRENT_VERSION = 2
    MAGIC = 0x424C4F4244455343

    @classmethod
    def is_blob_descriptor(cls, data: Any) -> bool:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            return False
        raw = bytes(data)
        if len(raw) < 9:
            return False

        version = raw[0]
        # Version 1 has no magic header, so it cannot be distinguished safely
        # from arbitrary inline video bytes in this heuristic.
        if version == 1 or version > cls.CURRENT_VERSION:
            return False

        try:
            return struct.unpack("<Q", raw[1:9])[0] == cls.MAGIC
        except Exception:
            return False


def is_blob_descriptor(data: Any) -> bool:
    return _BlobDescriptorProbe.is_blob_descriptor(data)


def open_blob_descriptor_stream(
    raw_value: bytes,
    blob_reader_registry=None,
) -> BinaryIO:
    if blob_reader_registry is not None:
        stream = blob_reader_registry.open_blob_descriptor_stream(raw_value)
        if stream is not None:
            return stream

    if _BlobDescriptorProbe.is_blob_descriptor(raw_value):
        raise RuntimeError(
            "BlobDescriptor input requires a registered Paimon table FileIO"
        )
    return io.BytesIO(bytes(raw_value))


def _decode_video_snapshot(
    stream: BinaryIO,
    image_format: str,
    timestamp_ms: int = 0,
) -> bytes | None:
    try:
        import av
    except ImportError as e:
        raise ImportError("PyAV is required to decode video snapshots") from e

    with av.open(stream, mode="r") as container:
        format_names = set((container.format.name or "").split(","))
        if format_names & _STILL_IMAGE_FORMATS:
            logger.debug(
                "video_snapshot input is a still image format: %s",
                container.format.name,
            )
            return None
        if not container.streams.video:
            return None

        target_seconds = timestamp_ms / 1000
        if timestamp_ms > 0:
            container.seek(timestamp_ms * 1000, backward=True, any_frame=False)

        candidate = None
        for frame in container.decode(video=0):
            if (
                timestamp_ms > 0
                and frame.time is not None
                and frame.time < target_seconds
            ):
                candidate = frame
                continue
            candidate = frame
            break

        if candidate is not None:
            return _encode_video_frame(candidate, image_format)
    return None


def _encode_video_frame(frame, image_format: str) -> bytes:
    try:
        image = frame.to_image()
    except ImportError as e:
        raise ImportError("Pillow is required to encode video frame images") from e
    output = io.BytesIO()
    image.save(output, format=image_format)
    return output.getvalue()


def _decode_video_frame(
    stream: BinaryIO,
    image_format: str,
    frame_index: int,
) -> bytes | None:
    try:
        import av
    except ImportError as e:
        raise ImportError("PyAV is required to decode video frames") from e

    with av.open(stream, mode="r") as container:
        format_names = set((container.format.name or "").split(","))
        if format_names & _STILL_IMAGE_FORMATS:
            logger.debug(
                "video_frame input is a still image format: %s",
                container.format.name,
            )
            return None
        if not container.streams.video:
            return None

        for index, frame in enumerate(container.decode(video=0)):
            if index == frame_index:
                return _encode_video_frame(frame, image_format)
    return None


def _make_video_snapshot(image_format: str = "PNG", blob_reader_registry=None):
    image_format = image_format.upper()

    def video_snapshot(values, timestamps_ms=None):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return video_snapshot results") from e

        frames = []
        raw_values = values.to_pylist()
        if timestamps_ms is None:
            timestamp_values = [0] * len(raw_values)
        else:
            timestamp_values = timestamps_ms.to_pylist()
        if len(timestamp_values) != len(raw_values):
            raise ValueError(
                "video_snapshot timestamp argument must have the same row count"
            )

        # v1 intentionally decodes rows serially; callers should filter or limit
        # large scans before applying video_snapshot.
        for raw_value, timestamp_ms in zip(raw_values, timestamp_values):
            if raw_value is None or timestamp_ms is None:
                frames.append(None)
                continue

            try:
                timestamp_ms = int(timestamp_ms)
                if timestamp_ms < 0:
                    frames.append(None)
                    continue
                stream = open_blob_descriptor_stream(raw_value, blob_reader_registry)
                try:
                    frames.append(
                        _decode_video_snapshot(stream, image_format, timestamp_ms)
                    )
                finally:
                    stream.close()
            except ImportError:
                raise
            except Exception as e:
                logger.warning("Failed to decode video snapshot: %s", e)
                frames.append(None)

        return pa.array(frames, type=pa.binary())

    return video_snapshot


def _make_video_frame(image_format: str = "PNG", blob_reader_registry=None):
    image_format = image_format.upper()

    def video_frame(values, frame_indices):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return video_frame results") from e

        frames = []
        raw_values = values.to_pylist()
        frame_index_values = frame_indices.to_pylist()
        if len(frame_index_values) != len(raw_values):
            raise ValueError("video_frame index argument must have the same row count")

        for raw_value, frame_index in zip(raw_values, frame_index_values):
            if raw_value is None or frame_index is None:
                frames.append(None)
                continue

            try:
                frame_index = int(frame_index)
                if frame_index < 0:
                    frames.append(None)
                    continue
                stream = open_blob_descriptor_stream(raw_value, blob_reader_registry)
                try:
                    frames.append(_decode_video_frame(stream, image_format, frame_index))
                finally:
                    stream.close()
            except ImportError:
                raise
            except Exception as e:
                logger.warning("Failed to decode video frame: %s", e)
                frames.append(None)

        return pa.array(frames, type=pa.binary())

    return video_frame
