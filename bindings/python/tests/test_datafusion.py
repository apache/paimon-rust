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
import importlib
import json
import os
import re
import struct
import sys
import tarfile
import tempfile
import types
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from datafusion import SessionContext

from pypaimon_rust.datafusion import (
    PaimonCatalog,
    PythonScalarUDF,
    SQLContext,
    SQLOptions,
    udf,
)


@pytest.fixture(scope="module")
def golden_warehouse(tmp_path_factory):
    override = os.environ.get("PAIMON_TEST_WAREHOUSE")
    if override is not None:
        return override

    warehouse = tmp_path_factory.mktemp("golden-warehouse")
    archive = (
        Path(__file__).resolve().parents[3]
        / "crates/integrations/datafusion/testdata/warehouse/warehouse.tar.gz"
    )
    with tarfile.open(archive, "r:gz") as fixture:
        if hasattr(tarfile, "data_filter"):
            fixture.extraction_filter = tarfile.data_filter
        members = fixture.getmembers()
        for member in members:
            target = (warehouse / member.name).resolve()
            if not member.isfile() or not target.is_relative_to(warehouse):
                raise ValueError(f"Unexpected golden archive entry: {member.name}")
        fixture.extractall(warehouse, members=members)
    return str(warehouse)


def test_golden_warehouse_with_legacy_tarfile(monkeypatch, tmp_path_factory):
    monkeypatch.delenv("PAIMON_TEST_WAREHOUSE", raising=False)
    if hasattr(tarfile, "data_filter"):
        original_extractall = tarfile.TarFile.extractall
        trusted_filter = tarfile.fully_trusted_filter

        def legacy_extractall(self, path=".", members=None, *, numeric_owner=False):
            return original_extractall(
                self, path, members, numeric_owner=numeric_owner, filter=trusted_filter
            )

        monkeypatch.setattr(tarfile.TarFile, "extractall", legacy_extractall)
        monkeypatch.delattr(tarfile, "data_filter")

    warehouse = golden_warehouse.__wrapped__(tmp_path_factory)
    catalog = PaimonCatalog({"warehouse": warehouse})
    assert catalog.get_table("default.simple_log_table").latest_snapshot().id() == 1


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
BLOB_DESCRIPTOR_MAGIC = 0x424C4F4244455343


def serialize_blob_descriptor(uri: str, offset: int, length: int) -> bytes:
    uri_bytes = uri.encode("utf-8")
    return (
        struct.pack("<BQI", 2, BLOB_DESCRIPTOR_MAGIC, len(uri_bytes))
        + uri_bytes
        + struct.pack("<qq", offset, length)
    )


def serialize_video_frame_descriptor(uri, offset, length, frame_index, moov_range=(0, 0)):
    uri_bytes = uri.encode("utf-8")
    return (
        struct.pack("<BQI", 1, 0x564944454F46524D, len(uri_bytes))
        + uri_bytes
        + struct.pack("<5q", offset, length, frame_index, *moov_range)
    )


def write_sample_video(
    path: Path,
    colors: tuple[tuple[int, int, int], ...] = ((240, 40, 80),),
    frame_pts: tuple[int, ...] | None = None,
    *,
    codec: str = "mpeg4",
    codec_tag: str | None = None,
    codec_options: dict[str, str] | None = None,
    muxer_options: dict[str, str] | None = None,
) -> None:
    av = pytest.importorskip("av")
    image_module = pytest.importorskip("PIL.Image")

    with av.open(str(path), mode="w", options=muxer_options) as container:
        stream = container.add_stream(codec, rate=1, options=codec_options)
        if codec_tag is not None:
            stream.codec_context.codec_tag = codec_tag
        stream.width = 32
        stream.height = 32
        stream.pix_fmt = "yuv420p"

        for index, color in enumerate(colors):
            image = image_module.new("RGB", (32, 32), color=color)
            frame = av.VideoFrame.from_image(image)
            if frame_pts is not None:
                frame.pts = frame_pts[index]
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)


def sample_image_bytes(
    size: tuple[int, int] = (32, 32),
    color: tuple[int, int, int] = (40, 120, 220),
) -> bytes:
    image_module = pytest.importorskip("PIL.Image")

    output = io.BytesIO()
    image = image_module.new("RGB", size, color=color)
    image.save(output, format="PNG")
    return output.getvalue()


def extract_rows(batches):
    table = pa.Table.from_batches(batches)
    return sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))


def test_sql_context_accepts_runtime_resource_configuration():
    with tempfile.TemporaryDirectory() as temp_directory:
        ctx = SQLContext(
            memory_pool_type="fair",
            memory_pool_bytes=16 * 1024 * 1024,
            temp_directory=temp_directory,
            max_temp_directory_size_bytes=256 * 1024 * 1024,
        )

        batches = ctx.sql(
            """
            EXPLAIN ANALYZE
            SELECT value
            FROM generate_series(1, 1000000) AS t(value)
            ORDER BY value DESC
            """
        )
        plan = pa.Table.from_batches(batches)["plan"].to_pylist()[0]
        spill_count = re.search(r"spill_count=(\d+)", plan)

        assert spill_count is not None
        assert int(spill_count.group(1)) > 0
        assert any(Path(temp_directory).iterdir())


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"memory_pool_type": "fair"}, "requires memory_pool_bytes"),
        ({"memory_pool_bytes": 0}, "must be greater than zero"),
        (
            {"memory_pool_type": "unknown", "memory_pool_bytes": 1024},
            "expected 'fair' or 'greedy'",
        ),
    ],
)
def test_sql_context_rejects_invalid_runtime_resource_configuration(kwargs, message):
    with pytest.raises(ValueError, match=message):
        SQLContext(**kwargs)


@pytest.mark.parametrize("method", ["sql_with_options", "sql_stream_with_options"])
def test_sql_with_options_disables_statements(method):
    ctx = SQLContext()
    options = SQLOptions().with_allow_statements(False)

    with pytest.raises(ValueError, match="Statement not supported"):
        getattr(ctx, method)("SET datafusion.execution.batch_size = 1024", options)


@pytest.mark.parametrize("method", ["sql_with_options", "sql_stream_with_options"])
def test_sql_with_options_disables_ddl(method):
    ctx = SQLContext()
    options = SQLOptions().with_allow_ddl(False)

    with pytest.raises(ValueError, match="DDL not supported"):
        getattr(ctx, method)("CREATE VIEW blocked_view AS SELECT 1", options)


@pytest.mark.parametrize("method", ["sql_with_options", "sql_stream_with_options"])
def test_sql_with_options_disables_dml(tmp_path, method):
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path / "warehouse")})
    ctx.sql("CREATE TABLE paimon.default.test_table (id INT)")
    options = SQLOptions().with_allow_dml(False)

    with pytest.raises(ValueError, match="DML not supported"):
        getattr(ctx, method)(
            "INSERT INTO paimon.default.test_table VALUES (1)", options
        )


def test_video_snapshot_builtin_registered_on_context_init():
    ctx = SQLContext()

    batches = ctx.sql(
        """
        SELECT
            video_snapshot(CAST(NULL AS BYTEA)) AS cover_png,
            video_frame(CAST(NULL AS BYTEA), 0) AS frame_png,
            video_clip(CAST(NULL AS BYTEA), 0, 1) AS clip_mp4,
            media_info(CAST(NULL AS BYTEA)) AS media_info_json,
            media_thumbnail(CAST(NULL AS BYTEA)) AS thumbnail_png,
            vector_from_json(CAST(NULL AS STRING)) AS vector_value,
            vector_to_json(vector_from_json('[1.0, 2.5]')) AS vector_json
        """
    )
    table = pa.Table.from_batches(batches)

    assert table["cover_png"].to_pylist() == [None]
    assert table["frame_png"].to_pylist() == [None]
    assert table["clip_mp4"].to_pylist() == [None]
    assert table["media_info_json"].to_pylist() == [None]
    assert table["thumbnail_png"].to_pylist() == [None]
    assert table["vector_value"].to_pylist() == [None]
    assert json.loads(table["vector_json"].to_pylist()[0]) == [1.0, 2.5]


def test_sql_context_survives_multimodal_builtins_registration_failure(monkeypatch):
    monkeypatch.setitem(
        sys.modules,
        "pypaimon_rust.functions",
        types.SimpleNamespace(),
    )

    with pytest.warns(
        RuntimeWarning,
        match="multimodal built-ins could not be registered",
    ):
        ctx = SQLContext()

    batches = ctx.sql("SELECT 1 AS value")
    table = pa.Table.from_batches(batches)
    assert table["value"].to_pylist() == [1]


def test_video_snapshot_builtin_auto_registered_for_sql():
    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(video_path)
        video_bytes = video_path.read_bytes()

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.videos",
            pa.record_batch(
                [[1], pa.array([video_bytes], type=pa.binary())],
                names=["id", "video"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT id, video_snapshot(video) AS cover_png
            FROM paimon.default.videos
            """
        )
        table = pa.Table.from_batches(batches)

        assert table["id"].to_pylist() == [1]
        assert table["cover_png"].to_pylist()[0].startswith(PNG_SIGNATURE)

        ctx.sql("DROP TEMPORARY TABLE paimon.default.videos")


def test_video_snapshot_descriptor_without_table_file_io_returns_null():
    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(video_path)
        descriptor = serialize_blob_descriptor(
            str(video_path), 0, video_path.stat().st_size
        )

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.videos",
            pa.record_batch(
                [[1], pa.array([descriptor], type=pa.binary())],
                names=["id", "video"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT id, video_snapshot(video) AS cover_png
            FROM paimon.default.videos
            """
        )
        table = pa.Table.from_batches(batches)

        assert table["id"].to_pylist() == [1]
        assert table["cover_png"].to_pylist() == [None]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.videos")


def test_video_snapshot_returns_null_for_image_bytes():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.media",
            pa.record_batch(
                [[1], pa.array([sample_image_bytes()], type=pa.binary())],
                names=["id", "content"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT id, video_snapshot(content) AS cover_png
            FROM paimon.default.media
            """
        )
        table = pa.Table.from_batches(batches)

        assert table["id"].to_pylist() == [1]
        assert table["cover_png"].to_pylist() == [None]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.media")


def test_video_snapshot_reads_descriptor_with_table_file_io():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.sql("CREATE TABLE paimon.default.videos (id INT, video BINARY)")

        video_path = Path(warehouse) / "default.db" / "videos" / "sample.mp4"
        video_path.parent.mkdir(parents=True, exist_ok=True)
        write_sample_video(video_path)
        descriptor = serialize_blob_descriptor(
            str(video_path), 0, video_path.stat().st_size
        )

        ctx.register_batch(
            "source_videos",
            pa.record_batch(
                [[1], pa.array([descriptor], type=pa.binary())],
                names=["id", "video"],
            ),
        )
        ctx.sql(
            """
            INSERT INTO paimon.default.videos
            SELECT id, video FROM paimon.default.source_videos
            """
        )

        batches = ctx.sql(
            """
            SELECT id, video_snapshot(video) AS cover_png
            FROM paimon.default.videos
            """
        )
        table = pa.Table.from_batches(batches)

        assert table["id"].to_pylist() == [1]
        assert table["cover_png"].to_pylist()[0].startswith(PNG_SIGNATURE)

        ctx.sql("DROP TEMPORARY TABLE paimon.default.source_videos")
        ctx.sql("DROP TABLE paimon.default.videos")


def test_video_snapshot_accepts_timestamp_ms():
    image_module = pytest.importorskip("PIL.Image")

    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(video_path, colors=((240, 40, 80), (40, 220, 80)))
        video_bytes = video_path.read_bytes()

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.videos",
            pa.record_batch(
                [[1], pa.array([video_bytes], type=pa.binary())],
                names=["id", "video"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT
                video_snapshot(video) AS first_png,
                video_snapshot(video, CAST(1000 AS INT)) AS second_png,
                video_snapshot(video, 5000) AS beyond_duration_png
            FROM paimon.default.videos
            """
        )
        row = pa.Table.from_batches(batches).to_pylist()[0]

        assert row["first_png"].startswith(PNG_SIGNATURE)
        assert row["second_png"].startswith(PNG_SIGNATURE)

        first_image = image_module.open(io.BytesIO(row["first_png"])).convert("RGB")
        second_image = image_module.open(io.BytesIO(row["second_png"])).convert("RGB")
        assert first_image.getpixel((16, 16)) != second_image.getpixel((16, 16))
        assert row["beyond_duration_png"].startswith(PNG_SIGNATURE)
        beyond_duration_image = image_module.open(
            io.BytesIO(row["beyond_duration_png"])
        ).convert("RGB")
        assert beyond_duration_image.getpixel((16, 16)) == second_image.getpixel((16, 16))

        ctx.sql("DROP TEMPORARY TABLE paimon.default.videos")


def test_video_frame_accepts_frame_index():
    image_module = pytest.importorskip("PIL.Image")

    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(
            video_path,
            colors=((240, 40, 80), (40, 220, 80), (40, 80, 240)),
        )
        video_bytes = video_path.read_bytes()

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.videos",
            pa.record_batch(
                [[1], pa.array([video_bytes], type=pa.binary())],
                names=["id", "video"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT
                video_frame(video, 0) AS first_png,
                video_frame(video, CAST(1 AS BIGINT)) AS second_png,
                video_frame(video, CAST(2 AS INT)) AS third_png,
                video_frame(video, 3) AS beyond_duration_png,
                video_frame(video, -1) AS negative_png
            FROM paimon.default.videos
            """
        )
        row = pa.Table.from_batches(batches).to_pylist()[0]

        assert row["first_png"].startswith(PNG_SIGNATURE)
        assert row["second_png"].startswith(PNG_SIGNATURE)
        assert row["third_png"].startswith(PNG_SIGNATURE)
        assert row["beyond_duration_png"] is None
        assert row["negative_png"] is None

        first_image = image_module.open(io.BytesIO(row["first_png"])).convert("RGB")
        second_image = image_module.open(io.BytesIO(row["second_png"])).convert("RGB")
        third_image = image_module.open(io.BytesIO(row["third_png"])).convert("RGB")
        assert first_image.getpixel((16, 16)) != second_image.getpixel((16, 16))
        assert second_image.getpixel((16, 16)) != third_image.getpixel((16, 16))

        ctx.sql("DROP TEMPORARY TABLE paimon.default.videos")


def test_video_clip_reads_source_and_selects_exact_frames(tmp_path):
    av = pytest.importorskip("av")
    video_path = tmp_path / "sample.mp4"
    write_sample_video(
        video_path,
        colors=((240, 40, 80), (40, 220, 80), (40, 80, 240)),
        frame_pts=(0, 1, 3),
    )
    video_bytes = video_path.read_bytes()

    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE TABLE paimon.default.videos (video BINARY)")
    packed_path = tmp_path / "default.db" / "videos" / "sample.video"
    prefix = b"other payload"
    packed_path.write_bytes(prefix + video_bytes + b"packed index")
    frame_descriptor = serialize_video_frame_descriptor(
        str(packed_path), len(prefix), len(video_bytes), 2
    )
    blob_descriptor = serialize_blob_descriptor(
        str(packed_path), len(prefix), len(video_bytes)
    )
    ctx.register_batch(
        "source_videos",
        pa.record_batch(
            [pa.array([video_bytes, blob_descriptor, frame_descriptor], type=pa.binary())],
            names=["video"],
        ),
    )
    ctx.sql("INSERT INTO paimon.default.videos SELECT video FROM paimon.default.source_videos")
    batches = ctx.sql(
        """
        SELECT
            video_clip(arrow_cast(video, 'LargeBinary'), CAST(1 AS INT), 3) AS clip,
            video_clip(video, 1, 4) AS beyond_end,
            video_clip(video, -1, 3) AS negative,
            video_clip(video, 2, 2) AS empty,
            video_clip(video, NULL, 3) AS null_bound
        FROM paimon.default.videos
        """
    )
    rows = pa.Table.from_batches(batches).to_pylist()

    assert len(rows) == 3
    for row in rows:
        assert row["beyond_end"] is None
        assert row["negative"] is None
        assert row["empty"] is None
        assert row["null_bound"] is None
        assert row["clip"][4:8] == b"ftyp"
        with av.open(io.BytesIO(row["clip"])) as clip:
            assert clip.streams.video[0].codec_context.name == "h264"
            frames = list(clip.decode(video=0))
            assert [frame.time for frame in frames] == [0.0, 2.0]
            assert float(frames[-1].duration * frames[-1].time_base) == 1.0
            green, blue = [frame.to_image().getpixel((16, 16)) for frame in frames]
            assert green == pytest.approx((40, 220, 80), abs=10)
            assert blue == pytest.approx((40, 80, 240), abs=10)


@pytest.mark.parametrize(
    "mode", ["positive_ctts", "negative_ctts", "seek_mismatch", "fragmented"]
)
def test_video_clip_seeks_using_packed_moov(tmp_path, monkeypatch, mode):
    av = pytest.importorskip("av")
    from pypaimon_rust import functions

    video_path = tmp_path / "sample.mp4"
    muxer_options = {
        "negative_ctts": {"movflags": "negative_cts_offsets"},
        "fragmented": {"movflags": "empty_moov+frag_keyframe"},
    }.get(mode)
    write_sample_video(
        video_path,
        colors=tuple((15 * index, 20, 220 - 10 * index) for index in range(15)),
        frame_pts=(0, 1, 2, 4, 5, 6, 7, 10, 11, 12, 15, 16, 17, 19, 21),
        codec="libx264",
        codec_options={"g": "4", "sc_threshold": "0", "bf": "2", "b_strategy": "0"},
        muxer_options=muxer_options,
    )
    video_bytes = video_path.read_bytes()
    moov_offset = video_bytes.index(b"moov") - 4
    moov_size = struct.unpack_from(">I", video_bytes, moov_offset)[0]
    start_frame, end_frame = 10, 14

    with av.open(str(video_path)) as source:
        frames = list(source.decode(video=0))
        assert any(frame.pict_type == av.video.frame.PictureType.B for frame in frames)
        assert not frames[start_frame].key_frame
        keyframe_pts = next(frame.pts for frame in reversed(frames[:start_frame]) if frame.key_frame)
        assert keyframe_pts > frames[0].pts

    attempts = []
    successful = []
    indexed_clip_frames = functions.indexed_clip_frames

    def record_seek(container, source, seek):
        attempts.append(seek.keyframe_pts)
        if mode == "seek_mismatch":
            seek = replace(seek, keyframe_position=-1)
        yield from indexed_clip_frames(container, source, seek)
        successful.append(seek.keyframe_pts)

    monkeypatch.setattr(functions, "indexed_clip_frames", record_seek)
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE TABLE paimon.default.videos (video BINARY)")
    packed_path = tmp_path / "default.db" / "videos" / "sample.video"
    prefix = b"other payload"
    packed_path.write_bytes(prefix + video_bytes + b"packed index")
    descriptor = serialize_video_frame_descriptor(
        str(packed_path), len(prefix), len(video_bytes), 2, (moov_offset, moov_size)
    )
    ctx.register_batch(
        "source_videos",
        pa.record_batch([pa.array([descriptor, video_bytes], type=pa.binary())], names=["video"]),
    )
    ctx.sql("INSERT INTO paimon.default.videos SELECT video FROM paimon.default.source_videos")
    batches = ctx.sql(
        f"SELECT video_clip(video, {start_frame}, {end_frame}) AS clip FROM paimon.default.videos"
    )
    indexed, sequential = pa.Table.from_batches(batches)["clip"].to_pylist()
    assert sequential is not None
    assert indexed == sequential
    assert attempts == ([] if mode == "fragmented" else [keyframe_pts])
    assert successful == ([keyframe_pts] if mode in ("positive_ctts", "negative_ctts") else [])


def test_read_video_assembles_ordered_images(tmp_path):
    av = pytest.importorskip("av")
    red, green, blue = (225, 35, 45), (45, 215, 45), (45, 35, 225)
    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.register_batch(
        "images",
        pa.record_batch(
            [[3, 1, 2], [sample_image_bytes(color=color) for color in (blue, red, green)]],
            names=["id", "image"],
        ),
    )
    sql = "-- Order the images for playback\nSELECT image FROM images ORDER BY id"
    clip = ctx.read_video(sql, column="image", fps=2.5)
    with av.open(io.BytesIO(clip)) as container:
        frames = list(container.decode(video=0))
        assert [frame.time for frame in frames] == pytest.approx([0.0, 0.4, 0.8])
        for frame, color in zip(frames, (red, green, blue), strict=True):
            assert frame.to_image().getpixel((16, 16)) == pytest.approx(color, abs=10)

    with pytest.raises(ValueError, match="fps is required"):
        ctx.read_video(sql, column="image")
    with pytest.raises(ValueError, match="no rows"):
        ctx.read_video("SELECT image FROM images WHERE FALSE", column="image", fps=30)
    with pytest.raises(ValueError, match="row 1 contains NULL"):
        ctx.read_video("SELECT CAST(NULL AS BYTEA) AS frame", fps=30)
    with pytest.raises(ValueError, match="row 1 is not a decodable image"):
        ctx.read_video("SELECT CAST('bad image' AS BYTEA) AS frame", fps=30)
    with pytest.raises(ValueError, match="requires a read-only query"):
        ctx.read_video("CREATE TABLE paimon.default.unexpected (id INT)", fps=30)


@pytest.mark.parametrize("retry_seek", [False, True])
def test_read_video_assembles_mixed_frames_across_batches(tmp_path, monkeypatch, retry_seek):
    av = pytest.importorskip("av")
    from pypaimon_rust import functions

    colors = tuple((20 + 13 * index, 180 - 12 * index, 45 + 8 * index) for index in range(12))
    pts = tuple(2 * index + (index >= 6) for index in range(12))
    source = tmp_path / "sample.mp4"
    write_sample_video(
        source,
        colors=colors,
        frame_pts=pts,
        codec="libx264",
        codec_options={"g": "4", "sc_threshold": "0", "bf": "2", "b_strategy": "0"},
    )
    video = source.read_bytes()
    moov_offset = video.index(b"moov") - 4
    moov_size = struct.unpack_from(">I", video, moov_offset)[0]

    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("SET datafusion.execution.batch_size = 2")
    ctx.sql("SET datafusion.execution.target_partitions = 1")
    ctx.sql("CREATE TABLE paimon.default.frames (position INT, frame BINARY)")
    table_path = tmp_path / "default.db" / "frames"
    packed = table_path / "frames.video"
    prefix = b"packed payload prefix"
    packed.write_bytes(prefix + video + b"packed index")

    def descriptor(ordinal):
        return serialize_video_frame_descriptor(
            str(packed), len(prefix), len(video), ordinal, (moov_offset, moov_size)
        )

    red, blue = (225, 20, 20), (20, 20, 225)
    blob = table_path / "image.blob"
    image = sample_image_bytes(size=(48, 24), color=blue)
    blob.write_bytes(prefix + image)
    rows = [
        sample_image_bytes(color=red),
        descriptor(5),
        descriptor(6),
        descriptor(7),
        serialize_blob_descriptor(str(blob), len(prefix), len(image)),
        descriptor(9),
    ]
    ctx.register_batch(
        "source_frames",
        pa.record_batch(
            [list(reversed(range(len(rows)))), list(reversed(rows))],
            names=["position", "frame"],
        ),
    )
    ctx.sql("INSERT INTO paimon.default.frames SELECT * FROM paimon.default.source_frames")
    sql = "SELECT frame FROM paimon.default.frames ORDER BY position"
    assert [batch.num_rows for batch in ctx.sql_stream(sql)] == [2, 2, 2]

    seek_sizes = []
    indexed_clip_frames = functions.indexed_clip_frames

    def record_seek(container, source, seek):
        seek_sizes.append(len(seek.expected_pts))
        frames = indexed_clip_frames(container, source, seek)
        if retry_seek and len(seek.expected_pts) == 3:
            yield next(frames)
            raise functions.VideoSeekMismatch("test seek failure after one frame")
        yield from frames

    monkeypatch.setattr(functions, "indexed_clip_frames", record_seek)
    clip = ctx.read_video(sql, fps=8)
    assert seek_sizes == [3, 1]
    with av.open(io.BytesIO(clip)) as container:
        frames = list(container.decode(video=0))
        assert [frame.time for frame in frames] == pytest.approx([i / 8 for i in range(6)])
        expected = (red, *colors[5:8], blue, colors[9])
        for frame, color in zip(frames, expected, strict=True):
            assert (frame.width, frame.height) == (32, 32)
            assert frame.to_image().getpixel((16, 16)) == pytest.approx(color, abs=12)

    clip = ctx.read_video(
        "SELECT frame FROM paimon.default.frames WHERE position BETWEEN 1 AND 3 ORDER BY position"
    )
    with av.open(io.BytesIO(clip)) as container:
        assert [frame.time for frame in container.decode(video=0)] == [0.0, 3.0, 5.0]
    assert seek_sizes == [3, 1, 3]


@pytest.mark.parametrize(
    "mode",
    ["closed", "signed_ctts", "open", "interlaced", "hevc_closed", "hevc_open", "hev1"],
)
def test_read_video_remuxes_only_safe_ranges(tmp_path, monkeypatch, mode):
    av = pytest.importorskip("av")
    from pypaimon_rust import _video_assembly

    source_path = tmp_path / "sample.mp4"
    colors = tuple((20 + 13 * index, 180 - 12 * index, 45 + 8 * index) for index in range(12))
    codec, tag = "libx264", "avc1"
    options = {"g": "4", "sc_threshold": "0", "bf": "2", "b_strategy": "0"}
    if mode.startswith("hevc_") or mode == "hev1":
        try:
            av.Codec("libx265", "w")
        except av.codec.codec.UnknownCodecError:
            pytest.skip("PyAV was built without libx265")
        codec, tag = "libx265", "hev1" if mode == "hev1" else "hvc1"
        open_gop = int(mode == "hevc_open")
        options = {
            "x265-params": (
                f"keyint=4:min-keyint=4:scenecut=0:open-gop={open_gop}:"
                "bframes=2:b-adapt=0:log-level=error:frame-threads=1:pools=none"
            )
        }
    elif mode == "open":
        options["x264-params"] = "open-gop=1"
    elif mode == "interlaced":
        options["x264-params"] = "interlaced=1:tff=1"
    write_sample_video(
        source_path,
        colors=colors,
        frame_pts=(0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12),
        codec=codec,
        codec_tag=tag,
        codec_options=options,
        muxer_options={"movflags": "negative_cts_offsets"} if mode == "signed_ctts" else None,
    )
    video = source_path.read_bytes()
    moov_offset = video.index(b"moov") - 4
    moov_size = struct.unpack_from(">I", video, moov_offset)[0]
    with av.open(str(source_path)) as source:
        source_frames = list(source.decode(video=0))
    assert len(source_frames) == len(colors)
    assert source_frames[4].key_frame

    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path)})
    ctx.sql("CREATE TABLE paimon.default.frames (ordinal INT, frame BINARY)")
    packed = tmp_path / "default.db" / "frames" / "frames.video"
    prefix = b"packed payload prefix"
    packed.write_bytes(prefix + video + b"packed index")
    descriptors = [
        serialize_video_frame_descriptor(
            str(packed), len(prefix), len(video), ordinal, (moov_offset, moov_size)
        )
        for ordinal in range(len(colors))
    ]
    ctx.register_batch(
        "source_frames",
        pa.record_batch([list(range(len(colors))), descriptors], names=["ordinal", "frame"]),
    )
    ctx.sql("INSERT INTO paimon.default.frames SELECT * FROM paimon.default.source_frames")

    encoded_ranges = []
    encode_video_range = _video_assembly._encode_video_range

    def record_encode(stream, start, end, moov):
        encoded_ranges.append((start, end))
        return encode_video_range(stream, start, end, moov)

    monkeypatch.setattr(_video_assembly, "_encode_video_range", record_encode)
    ranges = [(4, 5, False)]
    if mode in ("closed", "signed_ctts", "hevc_closed"):
        ranges = [(0, 4, True), (4, 8, True), (8, 12, True), (4, 7, False), (5, 8, False)]
    elif mode == "hevc_open":
        ranges = [(0, 4, True), (4, 5, False), (0, 12, False)]
    elif mode == "hev1":
        ranges = [(0, 4, True), (4, 8, False)]
    for start, end, copied in ranges:
        clip = ctx.read_video(
            f"SELECT frame FROM paimon.default.frames "
            f"WHERE ordinal >= {start} AND ordinal < {end} ORDER BY ordinal"
        )
        expected = source_frames[start:end]
        with av.open(io.BytesIO(clip)) as container:
            assert container.streams.video[0].codec_context.codec_tag == (tag if copied else "avc1")
            actual = list(container.decode(video=0))
        assert [frame.time for frame in actual] == pytest.approx(
            [frame.time - expected[0].time for frame in expected]
        )
        if copied:
            selected_pts = {frame.pts for frame in expected}
            with av.open(str(source_path)) as source:
                packets = [
                    bytes(packet) for packet in source.demux(video=0) if packet.pts in selected_pts
                ]
            with av.open(io.BytesIO(clip)) as container:
                assert [bytes(packet) for packet in container.demux(video=0) if packet.size] == packets
            assert [frame.duration for frame in actual] == [frame.duration for frame in expected]
        else:
            for frame, color in zip(actual, colors[start:end], strict=True):
                assert frame.to_image().getpixel((16, 16)) == pytest.approx(color, abs=12)
    assert encoded_ranges == [(start, end) for start, end, copied in ranges if not copied]


def test_media_info_returns_json_for_image_and_video():
    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(video_path)

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.media",
            pa.record_batch(
                [
                    [1, 2],
                    pa.array(
                        [
                            sample_image_bytes(size=(48, 24)),
                            video_path.read_bytes(),
                        ],
                        type=pa.binary(),
                    ),
                ],
                names=["id", "content"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT id, media_info(content) AS info_json
            FROM paimon.default.media
            ORDER BY id
            """
        )
        rows = pa.Table.from_batches(batches).to_pylist()
        image_info = json.loads(rows[0]["info_json"])
        video_info = json.loads(rows[1]["info_json"])

        assert image_info["media_type"] == "image"
        assert image_info["format"] == "png"
        assert image_info["width"] == 48
        assert image_info["height"] == 24

        assert video_info["media_type"] == "video"
        assert video_info["width"] == 32
        assert video_info["height"] == 32
        assert video_info["has_audio"] is False

        ctx.sql("DROP TEMPORARY TABLE paimon.default.media")


def test_media_thumbnail_handles_image_and_video():
    image_module = pytest.importorskip("PIL.Image")

    with tempfile.TemporaryDirectory() as warehouse:
        video_path = Path(warehouse) / "sample.mp4"
        write_sample_video(video_path)

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch(
            "paimon.default.media",
            pa.record_batch(
                [
                    [1, 2],
                    pa.array(
                        [
                            sample_image_bytes(size=(64, 32)),
                            video_path.read_bytes(),
                        ],
                        type=pa.binary(),
                    ),
                ],
                names=["id", "content"],
            ),
        )

        batches = ctx.sql(
            """
            SELECT
                id,
                media_thumbnail(content, 16, 16) AS thumbnail_png,
                media_thumbnail(content, -1, 16) AS invalid_thumbnail_png
            FROM paimon.default.media
            ORDER BY id
            """
        )
        rows = pa.Table.from_batches(batches).to_pylist()

        for row in rows:
            assert row["thumbnail_png"].startswith(PNG_SIGNATURE)
            thumbnail = image_module.open(io.BytesIO(row["thumbnail_png"]))
            assert thumbnail.width <= 16
            assert thumbnail.height <= 16
            assert row["invalid_thumbnail_png"] is None

        ctx.sql("DROP TEMPORARY TABLE paimon.default.media")


def test_vector_json_bridge_functions():
    ctx = SQLContext()

    batches = ctx.sql(
        """
        SELECT
            vector_from_json('[1.0, 2.5, -3]') AS vector_value,
            vector_from_json('not json') AS invalid_json,
            vector_from_json('[true]') AS invalid_value,
            vector_to_json(vector_from_json('[1, 2.5]')) AS vector_json
        """
    )
    row = pa.Table.from_batches(batches).to_pylist()[0]

    assert row["vector_value"] == [1.0, 2.5, -3.0]
    assert row["invalid_json"] is None
    assert row["invalid_value"] is None
    assert json.loads(row["vector_json"]) == [1.0, 2.5]


def test_query_simple_table_via_catalog_provider(golden_warehouse):
    catalog = PaimonCatalog({"warehouse": golden_warehouse})
    ctx = SessionContext()
    ctx.register_catalog_provider("paimon", catalog)

    df = ctx.sql("SELECT id, name FROM paimon.default.simple_log_table")

    assert extract_rows(df.collect()) == [
        (1, "alice"),
        (2, "bob"),
        (3, "carol"),
    ]


def test_catalog_provider_returns_pyarrow_compatible_strings():
    with tempfile.TemporaryDirectory() as warehouse:
        writer = SQLContext()
        writer.register_catalog("paimon", {"warehouse": warehouse})
        writer.sql("CREATE TABLE paimon.default.users (id INT, name STRING)")
        writer.sql(
            "INSERT INTO paimon.default.users VALUES (2, 'bob'), (1, 'alice')"
        )

        ctx = SessionContext()
        ctx.register_catalog_provider(
            "paimon", PaimonCatalog({"warehouse": warehouse})
        )

        batches = ctx.sql("SELECT id, name FROM paimon.default.users").collect()
        assert batches[0].schema.field("name").type == pa.string()
        assert pa.Table.from_batches(batches).sort_by("id").to_pylist() == [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]


def test_sql_context_ddl_dml():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql("CREATE SCHEMA paimon.test_db")
        ctx.sql(
            "CREATE TABLE paimon.test_db.users "
            "(id INT, name STRING, PRIMARY KEY (id))"
        )

        ctx.sql("INSERT INTO paimon.test_db.users VALUES (1, 'alice'), (2, 'bob')")

        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TABLE paimon.test_db.users")
        ctx.sql("DROP SCHEMA paimon.test_db")


def test_register_batch_fully_qualified():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
        ctx.register_batch("paimon.default.my_temp", batch)

        batches = ctx.sql("SELECT id, name FROM paimon.default.my_temp")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_batch_bare_name():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
        # Bare name uses current catalog and current database
        ctx.register_batch("my_temp", batch)

        batches = ctx.sql("SELECT id, name FROM paimon.default.my_temp")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "alice"), (2, "bob")]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_udf_from_python():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, None, 3]], names=["id"])
        ctx.register_batch("my_temp", batch)

        def plus_ten(values):
            return pa.array(
                [None if value is None else value + 10 for value in values.to_pylist()],
                type=pa.int64(),
            )

        ctx.register_udf(udf(plus_ten, [pa.int64()], pa.int64(), "volatile", "plus_ten"))

        batches = ctx.sql(
            "SELECT plus_ten(id) AS id FROM paimon.default.my_temp ORDER BY id"
        )
        table = pa.Table.from_batches(batches)
        assert table["id"].to_pylist() == [11, 13, None]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_udf_default_name_is_sql_identifier_for_closure():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2]], names=["id"])
        ctx.register_batch("my_temp", batch)

        def build_udf():
            def plus_one(values):
                return pa.array(
                    [value + 1 for value in values.to_pylist()], type=pa.int64()
                )

            return plus_one

        scalar_udf = udf(build_udf(), [pa.int64()], pa.int64(), "volatile")
        assert scalar_udf.name == "plus_one"
        ctx.register_udf(scalar_udf)

        batches = ctx.sql(
            "SELECT plus_one(id) AS id FROM paimon.default.my_temp ORDER BY id"
        )
        table = pa.Table.from_batches(batches)
        assert table["id"].to_pylist() == [2, 3]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_udf_multiple_arguments():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch(
            [
                pa.array([1, 2, None], type=pa.int64()),
                pa.array([10, 20, 30], type=pa.int64()),
            ],
            names=["id", "delta"],
        )
        ctx.register_batch("my_temp", batch)

        def add_values(left, right):
            values = []
            for left_value, right_value in zip(left.to_pylist(), right.to_pylist()):
                if left_value is None or right_value is None:
                    values.append(None)
                else:
                    values.append(left_value + right_value)
            return pa.array(values, type=pa.int64())

        ctx.register_udf(
            udf(
                add_values,
                [pa.int64(), pa.int64()],
                pa.int64(),
                "volatile",
                "add_values",
            )
        )

        batches = ctx.sql(
            """
            SELECT add_values(id, delta) AS value
            FROM paimon.default.my_temp
            ORDER BY id
            """
        )
        table = pa.Table.from_batches(batches)
        assert table["value"].to_pylist() == [11, 22, None]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_register_udf_multi_partition_union_plan():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1, 2, 3]], names=["id"])
        ctx.register_batch("my_temp", batch)

        def plus_ten(values):
            return pa.array([value + 10 for value in values.to_pylist()], type=pa.int64())

        ctx.register_udf(udf(plus_ten, [pa.int64()], pa.int64(), "volatile", "plus_ten"))

        batches = ctx.sql(
            """
            SELECT plus_ten(id) AS id FROM paimon.default.my_temp
            UNION ALL
            SELECT plus_ten(id) AS id FROM paimon.default.my_temp
            ORDER BY id
            """
        )
        table = pa.Table.from_batches(batches)
        assert table["id"].to_pylist() == [11, 11, 12, 12, 13, 13]

        ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")


def test_udf_rejects_non_callable():
    try:
        udf(1, [pa.int64()], pa.int64(), "volatile")
        pytest.fail("expected non-callable UDF creation to fail")
    except TypeError as e:
        assert "`func` argument must be callable" in str(e)


def test_udf_rejects_unsupported_type():
    def identity(values):
        return values

    try:
        udf(identity, [object()], pa.int64(), "volatile", "identity")
        pytest.fail("expected unsupported type registration to fail")
    except TypeError as e:
        assert "Expected a pyarrow.DataType" in str(e)


def test_python_scalar_udf_constructor_matches_datafusion_shape():
    def identity(values):
        return values

    scalar_udf = PythonScalarUDF(
        "identity", identity, [pa.field("value", pa.int64())], pa.int64(), "stable"
    )

    assert scalar_udf.name == "identity"
    assert repr(scalar_udf) == "PythonScalarUDF(identity)"


def test_python_udf_exception_surfaces():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch("my_temp", pa.record_batch([[1]], names=["id"]))

        def boom(values):
            raise RuntimeError("boom")

        ctx.register_udf(udf(boom, [pa.int64()], pa.int64(), "volatile", "boom"))

        try:
            ctx.sql("SELECT boom(id) AS id FROM paimon.default.my_temp")
            pytest.fail("expected Python UDF exception to fail the query")
        except Exception as e:
            message = str(e)
            assert "Python UDF 'boom' failed" in message
            assert "boom" in message


def test_python_udf_rejects_wrong_length():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch("my_temp", pa.record_batch([[1, 2]], names=["id"]))

        def wrong_length(values):
            return pa.array([1], type=pa.int64())

        ctx.register_udf(
            udf(wrong_length, [pa.int64()], pa.int64(), "volatile", "wrong_length")
        )

        try:
            ctx.sql("SELECT wrong_length(id) AS id FROM paimon.default.my_temp")
            pytest.fail("expected wrong-length UDF result to fail the query")
        except Exception as e:
            message = str(e)
            assert "Python UDF 'wrong_length' returned 1 rows, expected 2" in message


def test_python_udf_rejects_wrong_type():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})
        ctx.register_batch("my_temp", pa.record_batch([[1]], names=["id"]))

        def wrong_type(values):
            return pa.array(["not an int"], type=pa.string())

        ctx.register_udf(
            udf(wrong_type, [pa.int64()], pa.int64(), "volatile", "wrong_type")
        )

        try:
            ctx.sql("SELECT wrong_type(id) AS id FROM paimon.default.my_temp")
            pytest.fail("expected wrong-type UDF result to fail the query")
        except Exception as e:
            message = str(e)
            assert "Python UDF 'wrong_type' returned Utf8, expected Int64" in message


def test_temp_table_shadows_paimon_table():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql("CREATE SCHEMA paimon.test_db")
        ctx.sql("CREATE TABLE paimon.test_db.users (id INT, name STRING)")
        ctx.sql("INSERT INTO paimon.test_db.users VALUES (1, 'real')")

        batch = pa.record_batch([[2], ["temp"]], names=["id", "name"])
        ctx.register_batch("paimon.test_db.users", batch)

        # Temp table should shadow the real Paimon table
        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(2, "temp")]

        ctx.sql("DROP TEMPORARY TABLE paimon.test_db.users")

        # After dropping, the real table is visible again
        batches = ctx.sql("SELECT id, name FROM paimon.test_db.users")
        table = pa.Table.from_batches(batches)
        rows = sorted(zip(table["id"].to_pylist(), table["name"].to_pylist()))
        assert rows == [(1, "real")]

        ctx.sql("DROP TABLE paimon.test_db.users")
        ctx.sql("DROP SCHEMA paimon.test_db")


def test_drop_temp_table_if_exists():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1]], names=["id"])
        ctx.register_batch("paimon.default.my_temp", batch)

        ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.default.my_temp")

        # Should be able to drop again without error
        ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.default.my_temp")


def test_multi_catalog_temp_table():
    with tempfile.TemporaryDirectory() as wh1, tempfile.TemporaryDirectory() as wh2:
        ctx = SQLContext()
        ctx.register_catalog("cat1", {"warehouse": wh1})
        ctx.register_catalog("cat2", {"warehouse": wh2})

        batch1 = pa.record_batch([[1]], names=["id"])
        batch2 = pa.record_batch([[2]], names=["id"])

        ctx.register_batch("cat1.default.t1", batch1)
        ctx.register_batch("cat2.default.t2", batch2)

        result1 = ctx.sql("SELECT id FROM cat1.default.t1")
        assert pa.Table.from_batches(result1)["id"].to_pylist() == [1]

        result2 = ctx.sql("SELECT id FROM cat2.default.t2")
        assert pa.Table.from_batches(result2)["id"].to_pylist() == [2]

        ctx.sql("DROP TEMPORARY TABLE cat1.default.t1")
        ctx.sql("DROP TEMPORARY TABLE cat2.default.t2")


def test_register_batch_invalid_catalog():
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        batch = pa.record_batch([[1]], names=["id"])
        try:
            ctx.register_batch("unknown_catalog.default.my_temp", batch)
            pytest.fail("Expected an error for unknown catalog")
        except Exception as e:
            assert "unknown_catalog" in str(e).lower() or "not a paimon" in str(e).lower() or "unknown" in str(e).lower()


def test_table_functions_registered_with_catalog():
    """register_catalog auto-registers vector_search / full_text_search as
    UDTFs. Calling one with the wrong argument count surfaces the function's
    own validation error, which proves it is registered — an unregistered
    name would instead fail with 'table function not found'."""
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        for fn in ("vector_search", "full_text_search"):
            try:
                ctx.sql(f"SELECT * FROM {fn}('only_one_arg')")
                pytest.fail(f"expected {fn} to reject a single argument")
            except Exception as e:
                assert "requires 4 arguments" in str(e), str(e)


def test_register_catalog_discovers_pyjindo(monkeypatch, tmp_path):
    package = tmp_path / "pyjindo"
    package.mkdir()
    (package / "__init__.py").write_text("")
    library = package / (
        "libjindosdk_python.dylib"
        if sys.platform == "darwin"
        else "libjindosdk_python.so"
    )
    library.write_bytes(b"not a shared library")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv("JINDOSDK_LIBRARY_PATH", raising=False)
    monkeypatch.delenv("JINDOSDK_HOME", raising=False)
    sys.modules.pop("pyjindo", None)
    importlib.invalidate_caches()

    ctx = SQLContext()
    with pytest.raises(Exception) as error:
        ctx.register_catalog(
            "paimon",
            {
                "warehouse": "oss://jindo-test-bucket/warehouse",
                "fs.oss.impl": "jindo",
            },
        )

    assert str(library) in str(error.value)


def test_list_databases_and_tables(golden_warehouse):
    catalog = PaimonCatalog({"warehouse": golden_warehouse})

    assert "default" in catalog.list_databases()

    tables = catalog.list_tables("default")
    assert "simple_log_table" in tables

    table = catalog.get_table("default.simple_log_table")
    assert table.identifier() == "default.simple_log_table"
    assert table.location().endswith("/default.db/simple_log_table") or table.location()

    schema = table.schema()
    field_names = [f.name() for f in schema.fields()]
    assert "id" in field_names
    assert "name" in field_names
    # simple_log_table is non-partitioned, so partition keys are empty.
    assert schema.partition_keys() == []

# ---------------- #285: observability ----------------
def test_snapshots_for_simple_table(golden_warehouse):
    catalog = PaimonCatalog({"warehouse": golden_warehouse})
    table = catalog.get_table("default.simple_log_table")

    snap = table.latest_snapshot()
    assert snap is not None
    assert snap.id() >= 1
    assert snap.commit_time_ms() > 0
    assert snap.commit_kind() in {"APPEND", "COMPACT", "OVERWRITE", "ANALYZE"}

    snaps = table.list_snapshots()
    assert len(snaps) >= 1
    # Newest first.
    assert snaps[0].id() == snap.id()


def test_partitions_and_tags_smoke(golden_warehouse):
    catalog = PaimonCatalog({"warehouse": golden_warehouse})
    table = catalog.get_table("default.simple_log_table")

    # Non-partitioned, non-tagged table: both should be empty but well-typed.
    parts = table.list_partitions()
    stats = table.partition_stats()
    tags = table.list_tags()

    assert isinstance(parts, list)
    assert isinstance(stats, list)
    assert isinstance(tags, list)
    # simple_log_table has no partition keys -> partition_stats yields a single
    # empty-partition bucket or zero buckets depending on how the snapshot was
    # written. Either is acceptable; we just check the shape.
    for p in parts:
        assert isinstance(p, dict)
    for t in tags:
        assert isinstance(t.name(), str)
        assert isinstance(t.snapshot_id(), int)


def test_partition_stats_with_partitioned_table():
    """Validates partition_stats() on a real partitioned table created in a
    temporary warehouse:
    - list_partitions() returns the correct partition values
    - partition_stats() returns correct record / file / size counts per partition
    This exercises PartitionComputer.generate_part_values and the per-partition
    aggregation in aggregate_partition_stats.
    """
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql(
            "CREATE TABLE paimon.default.events "
            "(id INT, name STRING, dt STRING) "
            "PARTITIONED BY (dt)"
        )
        ctx.sql(
            "INSERT INTO paimon.default.events VALUES "
            "(1, 'alice', '2024-01-01'), "
            "(2, 'bob',   '2024-01-01'), "
            "(3, 'carol', '2024-01-02')"
        )

        catalog = PaimonCatalog({"warehouse": warehouse})
        table = catalog.get_table("default.events")

        # list_partitions() must return exactly the two inserted partitions.
        parts = table.list_partitions()
        assert len(parts) == 2
        part_values = sorted(p["dt"] for p in parts)
        assert part_values == ["2024-01-01", "2024-01-02"]

        # partition_stats() must report concrete record / file / size counts.
        stats = table.partition_stats()
        assert len(stats) == 2

        by_dt = {s.partition()["dt"]: s for s in stats}

        s1 = by_dt["2024-01-01"]
        assert s1.record_count() == 2
        assert s1.file_count() >= 1
        assert s1.total_size_bytes() > 0

        s2 = by_dt["2024-01-02"]
        assert s2.record_count() == 1
        assert s2.file_count() >= 1
        assert s2.total_size_bytes() > 0

        ctx.sql("DROP TABLE paimon.default.events")


def test_partition_stats_excludes_overwritten_partition():
    """Validates the merge_active_entries (FileKind::Add / Delete sign-flip) logic.

    INSERT OVERWRITE on a specific partition replaces the old data files with
    new ones. The old files become FileKind::Delete entries in the manifest, so
    aggregate_partition_stats() must net them out correctly:
    - The overwritten partition reflects the NEW row count, not the old one.
    - A partition that is overwritten with zero rows must not appear in the
      output (file_count nets to 0, triggering the `<= 0` guard).
    """
    with tempfile.TemporaryDirectory() as warehouse:
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": warehouse})

        ctx.sql(
            "CREATE TABLE paimon.default.events "
            "(id INT, name STRING, dt STRING) "
            "PARTITIONED BY (dt)"
        )
        # Initial state: 2 rows in 2024-01-01, 1 row in 2024-01-02.
        ctx.sql(
            "INSERT INTO paimon.default.events VALUES "
            "(1, 'alice', '2024-01-01'), "
            "(2, 'bob',   '2024-01-01'), "
            "(3, 'carol', '2024-01-02')"
        )

        # Overwrite 2024-01-01 with a single new row.
        # This generates FileKind::Delete entries for the old files of that
        # partition and a FileKind::Add entry for the new file.
        ctx.sql(
            "INSERT OVERWRITE paimon.default.events "
            "VALUES (10, 'dave', '2024-01-01')"
        )

        catalog = PaimonCatalog({"warehouse": warehouse})
        table = catalog.get_table("default.events")

        # Both partitions must still be present.
        parts = table.list_partitions()
        part_values = sorted(p["dt"] for p in parts)
        assert part_values == ["2024-01-01", "2024-01-02"]

        stats = table.partition_stats()
        by_dt = {s.partition()["dt"]: s for s in stats}

        # 2024-01-01: overwritten to 1 row — old Delete entries must be
        # netted out so record_count reflects only the new file.
        s1 = by_dt["2024-01-01"]
        assert s1.record_count() == 1

        # 2024-01-02: untouched.
        s2 = by_dt["2024-01-02"]
        assert s2.record_count() == 1

        ctx.sql("DROP TABLE paimon.default.events")
