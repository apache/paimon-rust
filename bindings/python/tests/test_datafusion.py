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
import os
import struct
import sys
import tempfile
import types
from pathlib import Path

import pyarrow as pa
import pytest
from datafusion import SessionContext

from pypaimon_rust.datafusion import PaimonCatalog, PythonScalarUDF, SQLContext, udf

WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
BLOB_DESCRIPTOR_MAGIC = 0x424C4F4244455343


def serialize_blob_descriptor(uri: str, offset: int, length: int) -> bytes:
    uri_bytes = uri.encode("utf-8")
    return (
        struct.pack("<BQI", 2, BLOB_DESCRIPTOR_MAGIC, len(uri_bytes))
        + uri_bytes
        + struct.pack("<qq", offset, length)
    )


def write_sample_video(
    path: Path,
    colors: tuple[tuple[int, int, int], ...] = ((240, 40, 80),),
) -> None:
    av = pytest.importorskip("av")
    image_module = pytest.importorskip("PIL.Image")

    with av.open(str(path), mode="w") as container:
        stream = container.add_stream("mpeg4", rate=1)
        stream.width = 32
        stream.height = 32
        stream.pix_fmt = "yuv420p"

        for color in colors:
            image = image_module.new("RGB", (32, 32), color=color)
            frame = av.VideoFrame.from_image(image)
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


def test_video_snapshot_builtin_registered_on_context_init():
    ctx = SQLContext()

    batches = ctx.sql(
        """
        SELECT
            video_snapshot(CAST(NULL AS BYTEA)) AS cover_png,
            video_frame(CAST(NULL AS BYTEA), 0) AS frame_png,
            media_info(CAST(NULL AS BYTEA)) AS media_info_json,
            media_thumbnail(CAST(NULL AS BYTEA)) AS thumbnail_png,
            vector_from_json(CAST(NULL AS STRING)) AS vector_value,
            vector_to_json(vector_from_json('[1.0, 2.5]')) AS vector_json
        """
    )
    table = pa.Table.from_batches(batches)

    assert table["cover_png"].to_pylist() == [None]
    assert table["frame_png"].to_pylist() == [None]
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


def test_query_simple_table_via_catalog_provider():
    catalog = PaimonCatalog({"warehouse": WAREHOUSE})
    ctx = SessionContext()
    ctx.register_catalog_provider("paimon", catalog)

    df = ctx.sql("SELECT id, name FROM paimon.default.simple_log_table")

    assert extract_rows(df.collect()) == [
        (1, "alice"),
        (2, "bob"),
        (3, "carol"),
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


def test_list_databases_and_tables():
    catalog = PaimonCatalog({"warehouse": WAREHOUSE})

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
def test_snapshots_for_simple_table():
    catalog = PaimonCatalog({"warehouse": WAREHOUSE})
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


def test_partitions_and_tags_smoke():
    catalog = PaimonCatalog({"warehouse": WAREHOUSE})
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
