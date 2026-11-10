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

"""Map MP4 playback ordinals to a seek point using the packed video's moov."""

import struct
from dataclasses import dataclass
from fractions import Fraction
from itertools import chain, pairwise, repeat

_MAX_MOOV_SIZE = 32 * 1024 * 1024
_MAX_INDEXED_SAMPLES = 1_000_000


class VideoSeekMismatch(Exception):
    """The decoder did not produce the frames predicted by the MP4 index."""


@dataclass(frozen=True)
class _ClipSeek:
    timestamp: int
    keyframe_pts: int
    keyframe_position: int
    expected_pts: tuple[int, ...]


@dataclass(frozen=True)
class _SampleTimeline:
    presentation: list[tuple[int, int]]
    pts_shift: int
    seek_shift: int
    single_description: bool


@dataclass(frozen=True)
class _RemuxPlan:
    timestamp: int
    first_sample: int
    first_pts: int
    decode_pts: list[int]


def read_moov(stream, moov_range):
    offset, length = moov_range
    if not 8 <= length <= _MAX_MOOV_SIZE:
        return None
    try:
        stream.seek(offset)
        data = stream.read(length)
        return data if len(data) == length else None
    except (OSError, ValueError):
        return None
    finally:
        stream.seek(0)


def _boxes(data):
    offset = 0
    while offset < len(data):
        size, name = struct.unpack_from(">I4s", data, offset)
        header_size = 8
        if size == 1:
            size = struct.unpack_from(">Q", data, offset + 8)[0]
            header_size = 16
        elif size == 0:
            size = len(data) - offset
        if not header_size <= size <= len(data) - offset:
            raise ValueError("Invalid MP4 box size")
        yield name, data[offset + header_size : offset + size]
        offset += size


def _versioned_uint(box):
    # In tkhd, mvhd, and mdhd this field follows the two creation/modification times.
    version = box[0]
    if version not in (0, 1):
        raise ValueError("Unsupported MP4 box version")
    return struct.unpack_from(">I", box, 12 if version == 0 else 20)[0]


def _sample_offsets(ctts, sample_count):
    if ctts is None:
        return repeat(0, sample_count)
    version, entry_count = struct.unpack_from(">B3xI", ctts)
    if (
        version not in (0, 1)
        or entry_count > sample_count
        or len(ctts) != 8 + entry_count * 8
    ):
        raise ValueError("Invalid MP4 composition offsets")
    entries = list(struct.iter_unpack(">II" if version == 0 else ">Ii", ctts[8:]))
    if sum(count for count, _ in entries) != sample_count:
        raise ValueError("MP4 composition offset count differs from sample count")
    return chain.from_iterable(repeat(offset, count) for count, offset in entries)


def _single_edit(edts):
    edit = dict(_boxes(edts))[b"elst"]
    version, count = struct.unpack_from(">B3xI", edit)
    if count != 1 or version not in (0, 1):
        raise ValueError("Unsupported MP4 edit list")
    duration, media_time, rate = struct.unpack_from(">IiI" if version == 0 else ">QqI", edit, 8)
    if rate != 0x00010000 or media_time < 0:
        raise ValueError("Unsupported MP4 edit rate or media time")
    return media_time, duration


def _sample_timeline(source, moov):
    # AVC and HEVC MP4 tracks store one access unit per sample.
    if source.codec_context.name not in ("h264", "hevc") or not (
        0 < source.frames <= _MAX_INDEXED_SAMPLES
    ):
        return None
    entries = source.index_entries
    if len(entries) != source.frames or source.start_time is None:
        return None

    try:
        ((name, movie),) = _boxes(memoryview(moov))
        if name != b"moov":
            return None
        movie_boxes = list(_boxes(movie))
        tracks = []
        for name, box in movie_boxes:
            if name == b"mvex":
                return None
            if name == b"trak":
                track = dict(_boxes(box))
                if _versioned_uint(track[b"tkhd"]) == source.id:
                    tracks.append(track)
        (track,) = tracks
        media = dict(_boxes(track[b"mdia"]))
        media_scale = _versioned_uint(media[b"mdhd"])
        if source.time_base != Fraction(1, media_scale):
            return None
        sample_table = dict(_boxes(dict(_boxes(media[b"minf"]))[b"stbl"]))
        offsets = _sample_offsets(sample_table.get(b"ctts"), source.frames)
        stsd = sample_table.get(b"stsd", b"")
        single_description = len(stsd) >= 8 and struct.unpack_from(">I", stsd, 4)[0] == 1
        edit_start = edit_duration = None
        if b"edts" in track:
            edit_start, duration = _single_edit(track[b"edts"])
            movie_scale = _versioned_uint(dict(movie_boxes)[b"mvhd"])
            edit_duration = Fraction(duration * media_scale, movie_scale)
    except (KeyError, IndexError, ValueError, ZeroDivisionError, struct.error):
        return None

    # FFmpeg's index already incorporates the edit list in its decode timestamps.
    # Require a one-to-one mapping to the original samples before applying CTTS.
    presentation = []
    previous_dts = None
    for sample, (entry, offset) in enumerate(zip(entries, offsets, strict=True)):
        if entry.is_discard or (previous_dts is not None and entry.timestamp <= previous_dts):
            return None
        previous_dts = entry.timestamp
        presentation.append((entry.timestamp + offset, sample))
    presentation.sort()
    first_pts = presentation[0][0]
    first_dts = entries[0].timestamp
    if edit_start is None and presentation[0][1] != 0:
        return None
    if edit_start is not None and (
        source.start_time != 0
        or edit_start != first_pts - first_dts
        or presentation[-1][0] - first_pts >= edit_duration
    ):
        return None
    if any(current[0] == previous[0] for previous, current in pairwise(presentation)):
        return None

    # Negative CTTS offsets can make FFmpeg shift DTS. Align with its first PTS.
    shift = source.start_time - first_pts
    # The MOV demuxer searches the DTS index after undoing its timestamp shifts.
    # A full-track edit adds the correction that moved the first PTS to zero.
    seek_shift = shift + (max(0, -first_dts) if edit_start is not None else 0)
    return _SampleTimeline(presentation, shift, seek_shift, single_description)


def clip_seek(source, moov, start_frame, end_frame):
    if not 0 < start_frame < end_frame <= source.frames:
        return None
    timeline = _sample_timeline(source, moov)
    if timeline is None:
        return None
    presentation = timeline.presentation
    entries = source.index_entries
    for ordinal in range(start_frame, 0, -1):
        pts, sample = presentation[ordinal]
        entry = entries[sample]
        if entry.is_keyframe:
            return _ClipSeek(
                entry.timestamp + timeline.seek_shift,
                pts + timeline.pts_shift,
                entry.pos,
                tuple(pts + timeline.pts_shift for pts, _ in presentation[start_frame:end_frame]),
            )
    return None


def remux_plan(source, moov, start_frame, end_frame):
    if not 0 <= start_frame < end_frame <= source.frames:
        return None
    timeline = _sample_timeline(source, moov)
    if timeline is None or not timeline.single_description:
        return None
    first_pts, first_sample = timeline.presentation[start_frame]
    entry = source.index_entries[first_sample]
    if not entry.is_keyframe:
        return None

    # Every selected presentation frame must fit in one contiguous decode span.
    # A B-frame cut that crosses a dependency leaves a hole or falls outside it.
    decode_pts = [0] * (end_frame - start_frame)
    for ordinal in range(start_frame, end_frame):
        pts, sample = timeline.presentation[ordinal]
        index = sample - first_sample
        if not 0 <= index < len(decode_pts):
            return None
        decode_pts[index] = pts + timeline.pts_shift
    return _RemuxPlan(
        entry.timestamp + timeline.seek_shift,
        first_sample,
        first_pts + timeline.pts_shift,
        decode_pts,
    )


def indexed_clip_frames(container, source, seek):
    import av

    try:
        container.seek(seek.timestamp, stream=source, backward=True, any_frame=False)
        packets = iter(container.demux(source))
        first = next(packets, None)
        if (
            first is None
            or not first.is_keyframe
            or (first.pos, first.pts) != (seek.keyframe_position, seek.keyframe_pts)
        ):
            raise VideoSeekMismatch("MP4 seek landed on a different sample")

        index = 0
        for packet in chain((first,), packets):
            for frame in packet.decode():
                if frame.pts is None or frame.is_corrupt or frame.time_base != source.time_base:
                    raise VideoSeekMismatch("Invalid decoded frame after MP4 seek")
                if frame.pts < seek.expected_pts[0]:
                    continue
                if frame.pts != seek.expected_pts[index]:
                    raise VideoSeekMismatch("Decoded frame does not match the MP4 sample index")
                index += 1
                yield frame
                if index == len(seek.expected_pts):
                    return
    except (av.FFmpegError, OverflowError) as e:
        raise VideoSeekMismatch("MP4 seek or decode failed") from e
    raise VideoSeekMismatch("MP4 seek did not produce the complete clip")
