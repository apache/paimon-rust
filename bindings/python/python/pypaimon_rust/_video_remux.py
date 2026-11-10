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

"""Copy a verified, independently decodable video range into a new MP4."""

import io
from itertools import zip_longest

from ._video_index import remux_plan

_HEVC_IDR = (19, 20)
_HEVC_PARAMETER_SETS = frozenset((32, 33, 34))  # VPS, SPS, PPS


def _rbsp_reader(payload):
    rbsp = payload.replace(b"\x00\x00\x03", b"\x00\x00")
    bits = int.from_bytes(rbsp, "big")
    remaining = len(rbsp) * 8

    def read(size):
        nonlocal remaining
        remaining -= size
        if remaining < 0:
            raise ValueError("Truncated parameter set")
        return (bits >> remaining) & ((1 << size) - 1)

    def golomb():
        zeros = 0
        while not read(1):
            zeros += 1
        return (1 << zeros) - 1 + read(zeros)

    return read, golomb


def _progressive_sps(nal):
    if not 4 <= len(nal) <= 4096 or nal[0] & 31 != 7:
        return False
    read, golomb = _rbsp_reader(nal[1:])
    profile = read(8)
    read(16)  # constraint flags and level
    golomb()  # SPS ID
    if profile in (100, 110, 122, 244):
        if golomb() == 3:  # chroma_format_idc
            read(1)
        golomb()  # bit_depth_luma_minus8
        golomb()  # bit_depth_chroma_minus8
        read(1)
        if read(1):  # seq_scaling_matrix_present_flag
            return False
    elif profile not in (66, 77, 88):
        return False

    golomb()  # log2_max_frame_num_minus4
    poc_type = golomb()
    if poc_type == 0:
        golomb()  # log2_max_pic_order_cnt_lsb_minus4
    elif poc_type == 1:
        read(1)
        golomb()
        golomb()
        offsets = golomb()
        if offsets > 255:
            return False
        for _ in range(offsets):
            golomb()
    elif poc_type != 2:
        return False
    golomb()  # max_num_ref_frames
    read(1)  # gaps_in_frame_num_value_allowed_flag
    golomb()  # pic_width_in_mbs_minus1
    golomb()  # pic_height_in_map_units_minus1
    return bool(read(1))  # frame_mbs_only_flag


def _avc_nal_length_size(codec):
    # A single progressive SPS/PPS pair and one MP4 sample description make
    # each AVC access unit one frame with a fixed decoder configuration.
    if codec is None or codec.name != "h264" or codec.codec_tag != "avc1":
        return None
    config = codec.extradata
    if not config or len(config) < 11:
        return None
    if config[0] != 1 or config[5] & 31 != 1:
        return None
    pps_count_at = 8 + int.from_bytes(config[6:8], "big")
    if pps_count_at + 4 > len(config) or config[pps_count_at] != 1:
        return None
    pps_size = int.from_bytes(config[pps_count_at + 1 : pps_count_at + 3], "big")
    if not 0 < pps_size <= len(config) - pps_count_at - 3:
        return None
    if config[pps_count_at + 3] & 31 != 8 or not _progressive_sps(config[8:pps_count_at]):
        return None
    length_size = (config[4] & 3) + 1
    return length_size if length_size != 3 else None


def _nal_units(packet, length_size):
    data = memoryview(packet)
    position = 0
    while position < len(data):
        size = int.from_bytes(data[position : position + length_size], "big")
        position += length_size
        if not 0 < size <= len(data) - position or data[position] & 0x80:
            raise ValueError("Invalid NAL unit")
        yield data[position : position + size]
        position += size


def _avc_picture_type(packet, length_size):
    picture = None
    for nal in _nal_units(packet, length_size):
        nal_type = nal[0] & 31
        if nal_type in (1, 5):
            if picture is not None and picture != nal_type:
                return None
            picture = nal_type
        elif nal_type not in (6, 9, 12):  # SEI, access-unit delimiter, filler
            return None
    return picture


def _hevc_nal_type(nal):
    # The remux path handles single-layer pictures only.
    if len(nal) < 3 or nal[0] & 0x81 or not 1 <= nal[1] <= 7:
        raise ValueError("Invalid or layered HEVC NAL unit")
    return (nal[0] >> 1) & 63


def _hevc_nal_length_size(codec, start_frame):
    config = codec.extradata
    if codec.codec_tag not in ("hvc1", "hev1") or not config or len(config) < 23:
        return None
    # Require the hvcC progressive-source and frame-only constraints.
    if config[0] != 1 or config[6] & 0xD0 != 0x90:
        return None
    # Incomplete hev1 arrays cannot rule out earlier in-band reconfiguration.
    require_complete = codec.codec_tag == "hvc1" or start_frame != 0
    parameter_sets = set()
    position = 23
    for _ in range(config[22]):
        if position + 3 > len(config):
            return None
        header = config[position]
        nal_type = header & 63
        count = int.from_bytes(config[position + 1 : position + 3], "big")
        position += 3
        if nal_type in _HEVC_PARAMETER_SETS:
            if (
                count != 1
                or nal_type in parameter_sets
                or (require_complete and not header & 0x80)
            ):
                return None
            parameter_sets.add(nal_type)
        elif nal_type not in (39, 40):  # SEI
            return None
        for _ in range(count):
            size = int.from_bytes(config[position : position + 2], "big")
            position += 2
            if not 3 <= size <= len(config) - position:
                return None
            nal = config[position : position + size]
            position += size
            if _hevc_nal_type(nal) != nal_type:
                return None
            if nal_type == 34:
                if size > 4096:
                    return None
                read, golomb = _rbsp_reader(nal[2:])
                golomb()  # PPS ID
                golomb()  # SPS ID
                read(1)  # dependent_slice_segments_enabled_flag
                if read(1):  # output_flag_present_flag can suppress pictures
                    return None
    if position != len(config) or parameter_sets != _HEVC_PARAMETER_SETS:
        return None
    length_size = (config[21] & 3) + 1
    return length_size if length_size != 3 else None


def _hevc_picture_type(packet, length_size):
    picture = None
    for nal in _nal_units(packet, length_size):
        nal_type = _hevc_nal_type(nal)
        # Reject open-GOP CRA, leading RADL/RASL, and parameter-set changes.
        if nal_type < 6 or nal_type in _HEVC_IDR:
            first_slice = bool(nal[2] & 0x80)
            if first_slice != (picture is None) or picture not in (None, nal_type):
                return None
            if nal_type in _HEVC_IDR and nal[2] & 0x40:  # no_output_of_prior_pics_flag
                return None
            picture = nal_type
        elif nal_type not in (35, 38, 39, 40):  # AUD, filler, SEI
            return None
    return picture


def _copy_packets(av, container, source, plan, length_size, picture_type, idr_types, output):
    time_base = source.time_base
    timescale = str(time_base.denominator)
    if plan.first_sample:
        container.seek(plan.timestamp, stream=source, backward=True, any_frame=False)
    packets = iter(container.demux(source))
    timeline = []
    with av.open(
        output,
        mode="w",
        format="mp4",
        options={"video_track_timescale": timescale, "movie_timescale": timescale},
    ) as clip:
        video = clip.add_stream_from_template(source)
        video.time_base = time_base
        video.codec_context.codec_tag = source.codec_context.codec_tag
        for index, pts in enumerate(plan.decode_pts, plan.first_sample):
            packet = next(packets, None)
            entry = source.index_entries[index]
            if (
                packet is None
                or packet.is_discard
                or packet.is_corrupt
                or packet.time_base != time_base
                or (packet.pos, packet.dts, packet.size) != (entry.pos, entry.timestamp, entry.size)
                or packet.pts != pts
                or packet.duration <= 0
                or any(packet.iter_sidedata())
            ):
                return None
            picture = picture_type(packet, length_size)
            if picture is None or (
                index == plan.first_sample and (picture not in idr_types or not packet.is_keyframe)
            ):
                return None
            packet.pts -= plan.first_pts
            packet.dts -= plan.first_pts
            timeline.append((packet.pts, packet.dts, packet.duration))
            packet.stream = video
            clip.mux(packet)
    return timeline


def remux_video_range(stream, start_frame, end_frame, moov):
    try:
        import av
    except ImportError:
        return None

    try:
        with av.open(stream, mode="r") as container:
            if "mp4" not in (container.format.name or "").split(",") or not container.streams.video:
                return None
            source = container.streams.video[0]
            codec = source.codec_context
            if codec is not None and codec.name == "hevc":
                length_size = _hevc_nal_length_size(codec, start_frame)
                picture_type, idr_types = _hevc_picture_type, _HEVC_IDR
            else:
                length_size = _avc_nal_length_size(codec)
                picture_type, idr_types = _avc_picture_type, (5,)
            if length_size is None:
                return None
            plan = remux_plan(source, moov, start_frame, end_frame)
            if plan is None:
                return None
            time_base = source.time_base
            output = io.BytesIO()
            timeline = _copy_packets(
                av, container, source, plan, length_size, picture_type, idr_types, output
            )
            if timeline is None:
                return None

        # The MP4 muxer can change timestamps through edit lists or rescaling.
        # Verify the resulting packet timeline without decoding frames.
        output.seek(0)
        with av.open(output, mode="r") as clip:
            video = clip.streams.video[0]
            if video.time_base != time_base:
                return None
            packets = (packet for packet in clip.demux(video) if packet.size)
            for packet, expected in zip_longest(packets, timeline):
                if (
                    packet is None
                    or packet.is_discard
                    or packet.is_corrupt
                    or (packet.pts, packet.dts, packet.duration) != expected
                ):
                    return None
        return output.getvalue()
    except (av.FFmpegError, OSError, ValueError, OverflowError, IndexError):
        return None
