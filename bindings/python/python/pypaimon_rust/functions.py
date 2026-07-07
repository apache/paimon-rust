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
import json
import logging
import math
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
    return _encode_image(image, image_format)


def _encode_image(image, image_format: str) -> bytes:
    output = io.BytesIO()
    image.save(output, format=image_format)
    return output.getvalue()


def _rewind_stream(stream: BinaryIO) -> None:
    try:
        stream.seek(0)
    except Exception:
        pass


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _duration_millis(container, stream=None) -> int | None:
    duration = getattr(container, "duration", None)
    if duration is not None and duration >= 0:
        return int(round(duration / 1000))

    if stream is not None:
        stream_duration = getattr(stream, "duration", None)
        time_base = getattr(stream, "time_base", None)
        if stream_duration is not None and stream_duration >= 0 and time_base is not None:
            return int(round(float(stream_duration * time_base) * 1000))
    return None


def _codec_name(stream) -> str | None:
    codec_context = getattr(stream, "codec_context", None)
    return getattr(codec_context, "name", None)


def _average_rate(stream) -> float | None:
    rate = getattr(stream, "average_rate", None)
    if rate is None:
        return None
    try:
        return float(rate)
    except Exception:
        return None


def _frame_count(stream) -> int | None:
    frames = getattr(stream, "frames", None)
    if frames is None or frames <= 0:
        return None
    return int(frames)


def _decode_image_info(stream: BinaryIO) -> dict[str, Any] | None:
    try:
        from PIL import Image
    except ImportError as e:
        raise ImportError("Pillow is required to inspect image media") from e

    with Image.open(stream) as image:
        image.load()
        return _drop_none(
            {
                "media_type": "image",
                "format": image.format.lower() if image.format else None,
                "width": image.width,
                "height": image.height,
                "mode": image.mode,
            }
        )


def _decode_av_media_info(stream: BinaryIO) -> dict[str, Any] | None:
    try:
        import av
    except ImportError as e:
        raise ImportError("PyAV is required to inspect video or audio media") from e

    with av.open(stream, mode="r") as container:
        format_name = (container.format.name or "").lower() or None
        format_names = set((container.format.name or "").split(","))
        video_streams = list(container.streams.video)
        audio_streams = list(container.streams.audio)

        if video_streams:
            stream0 = video_streams[0]
            media_type = "image" if format_names & _STILL_IMAGE_FORMATS else "video"
            info = {
                "media_type": media_type,
                "format": format_name,
                "duration_ms": _duration_millis(container, stream0),
                "width": getattr(stream0, "width", None),
                "height": getattr(stream0, "height", None),
                "codec": _codec_name(stream0),
                "frame_count": _frame_count(stream0),
                "average_rate": _average_rate(stream0),
                "has_audio": bool(audio_streams),
            }
            return _drop_none(info)

        if audio_streams:
            stream0 = audio_streams[0]
            info = {
                "media_type": "audio",
                "format": format_name,
                "duration_ms": _duration_millis(container, stream0),
                "codec": _codec_name(stream0),
                "has_audio": True,
            }
            return _drop_none(info)
    return None


def _decode_media_info(stream: BinaryIO) -> str | None:
    try:
        info = _decode_image_info(stream)
        if info is not None:
            return _json_dumps(info)
    except ImportError:
        raise
    except Exception:
        _rewind_stream(stream)

    info = _decode_av_media_info(stream)
    return _json_dumps(info) if info is not None else None


def _positive_dimension(value: Any, default: int) -> int | None:
    try:
        dimension = default if value is None else int(value)
    except Exception:
        return None
    return dimension if dimension > 0 else None


def _thumbnail_image(image, image_format: str, max_width: int, max_height: int) -> bytes:
    try:
        from PIL import Image
    except ImportError as e:
        raise ImportError("Pillow is required to encode media thumbnails") from e

    thumbnail = image.copy()
    resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS", None)
    if resampling is None:
        thumbnail.thumbnail((max_width, max_height))
    else:
        thumbnail.thumbnail((max_width, max_height), resampling)
    return _encode_image(thumbnail, image_format)


def _decode_image_thumbnail(
    stream: BinaryIO,
    image_format: str,
    max_width: int,
    max_height: int,
) -> bytes | None:
    try:
        from PIL import Image
    except ImportError as e:
        raise ImportError("Pillow is required to decode image thumbnails") from e

    with Image.open(stream) as image:
        image.load()
        return _thumbnail_image(image, image_format, max_width, max_height)


def _decode_video_thumbnail(
    stream: BinaryIO,
    image_format: str,
    max_width: int,
    max_height: int,
) -> bytes | None:
    try:
        import av
    except ImportError as e:
        raise ImportError("PyAV is required to decode video thumbnails") from e

    with av.open(stream, mode="r") as container:
        if not container.streams.video:
            return None
        for frame in container.decode(video=0):
            try:
                image = frame.to_image()
            except ImportError as e:
                raise ImportError("Pillow is required to encode video thumbnails") from e
            return _thumbnail_image(image, image_format, max_width, max_height)
    return None


def _decode_media_thumbnail(
    stream: BinaryIO,
    image_format: str,
    max_width: int,
    max_height: int,
) -> bytes | None:
    try:
        thumbnail = _decode_image_thumbnail(stream, image_format, max_width, max_height)
        if thumbnail is not None:
            return thumbnail
    except ImportError:
        raise
    except Exception:
        _rewind_stream(stream)

    return _decode_video_thumbnail(stream, image_format, max_width, max_height)


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


def _make_media_info(blob_reader_registry=None):
    def media_info(values):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return media_info results") from e

        infos = []
        for raw_value in values.to_pylist():
            if raw_value is None:
                infos.append(None)
                continue

            try:
                stream = open_blob_descriptor_stream(raw_value, blob_reader_registry)
                try:
                    infos.append(_decode_media_info(stream))
                finally:
                    stream.close()
            except ImportError:
                raise
            except Exception as e:
                logger.warning("Failed to inspect media: %s", e)
                infos.append(None)

        return pa.array(infos, type=pa.string())

    return media_info


def _make_media_thumbnail(
    image_format: str = "PNG",
    blob_reader_registry=None,
    default_max_width: int = 320,
    default_max_height: int = 320,
):
    image_format = image_format.upper()

    def media_thumbnail(values, max_widths=None, max_heights=None):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return media_thumbnail results") from e

        thumbnails = []
        raw_values = values.to_pylist()
        if max_widths is None and max_heights is None:
            width_values = [default_max_width] * len(raw_values)
            height_values = [default_max_height] * len(raw_values)
        elif max_widths is not None and max_heights is not None:
            width_values = max_widths.to_pylist()
            height_values = max_heights.to_pylist()
        else:
            raise ValueError("media_thumbnail requires both width and height arguments")
        if len(width_values) != len(raw_values) or len(height_values) != len(raw_values):
            raise ValueError("media_thumbnail size arguments must have the same row count")

        for raw_value, max_width, max_height in zip(
            raw_values, width_values, height_values
        ):
            if raw_value is None or max_width is None or max_height is None:
                thumbnails.append(None)
                continue

            max_width = _positive_dimension(max_width, default_max_width)
            max_height = _positive_dimension(max_height, default_max_height)
            if max_width is None or max_height is None:
                thumbnails.append(None)
                continue

            try:
                stream = open_blob_descriptor_stream(raw_value, blob_reader_registry)
                try:
                    thumbnails.append(
                        _decode_media_thumbnail(
                            stream, image_format, max_width, max_height
                        )
                    )
                finally:
                    stream.close()
            except ImportError:
                raise
            except Exception as e:
                logger.warning("Failed to decode media thumbnail: %s", e)
                thumbnails.append(None)

        return pa.array(thumbnails, type=pa.binary())

    return media_thumbnail


def _coerce_vector(value: Any) -> list[float] | None:
    if value is None or not isinstance(value, list):
        return None

    vector = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return None
        item = float(item)
        if not math.isfinite(item):
            return None
        vector.append(item)
    return vector


def _make_vector_from_json():
    def vector_from_json(values):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return vector_from_json results") from e

        vectors = []
        for raw_value in values.to_pylist():
            if raw_value is None:
                vectors.append(None)
                continue

            try:
                parsed = json.loads(raw_value)
                vectors.append(_coerce_vector(parsed))
            except Exception:
                vectors.append(None)

        return pa.array(vectors, type=pa.list_(pa.float32()))

    return vector_from_json


def _make_vector_to_json():
    def vector_to_json(values):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to return vector_to_json results") from e

        encoded = []
        for raw_value in values.to_pylist():
            vector = _coerce_vector(raw_value)
            encoded.append(_json_dumps(vector) if vector is not None else None)

        return pa.array(encoded, type=pa.string())

    return vector_to_json
