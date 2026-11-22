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

import datetime
import json
import struct
from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon_rust.data import variant_get, variant_get_numeric_fields
from pypaimon_rust.datafusion import SQLContext


class _LogicalIntType(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.int64(), "pypaimon.test.logical_int")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def _column(values, metadata=b"\x01\x00\x00"):
    return pa.StructArray.from_arrays(
        [pa.array(values, pa.binary()), pa.array([metadata] * len(values), pa.binary())],
        names=["value", "metadata"],
        mask=pa.array([value is None for value in values], type=pa.bool_()),
    )


def _json_column(value):
    literal = json.dumps(value).replace("'", "''")
    return SQLContext().sql(f"SELECT parse_json('{literal}') AS v")[0].column(0)


@pytest.mark.parametrize(
    "encoded,data_type,expected",
    [
        (b"\x04", pa.bool_(), True),
        *[
            (b"\x0c\xf4", dtype, -12)
            for dtype in (pa.int8(), pa.int16(), pa.int32(), pa.int64())
        ],
        (b"\x38" + struct.pack("<f", 1.25), pa.float32(), 1.25),
        (b"\x1c" + struct.pack("<d", 1.25), pa.float64(), 1.25),
        *[(b"\x0dabc", dtype, "abc") for dtype in (pa.string(), pa.large_string())],
        *[
            (b"\x3c\x03\x00\x00\x00abc", dtype, b"abc")
            for dtype in (pa.binary(), pa.large_binary())
        ],
        (b"\x2c\x01\x00\x00\x00", pa.date32(), datetime.date(1970, 1, 2)),
        (
            b"\x34" + struct.pack("<q", 1234567), pa.timestamp("us"),
            datetime.datetime(1970, 1, 1, 0, 0, 1, 234567),
        ),
        (
            b"\x30" + struct.pack("<q", 1234567), pa.timestamp("us", "Asia/Shanghai"),
            datetime.datetime(1970, 1, 1, 0, 0, 1, 234567, tzinfo=datetime.timezone.utc),
        ),
        (b"\x20\x02" + struct.pack("<i", 1230), pa.decimal128(4, 2), Decimal("12.30")),
        (
            b"\x28\x00" + (10**38 - 1).to_bytes(16, "little", signed=True),
            pa.decimal128(38, 0), Decimal("9" * 38),
        ),
    ],
)
def test_variant_get_exact_primitive_types(encoded, data_type, expected):
    result = variant_get(_column([encoded, b"\x00", None]), "$", data_type)
    assert result.type == data_type
    assert result.to_pylist() == [expected, None, None]


@pytest.mark.parametrize(
    "data_type",
    [pa.list_(pa.int64()), pa.large_list(pa.int64()), pa.list_(pa.int64(), 3)],
)
def test_variant_get_nested_lists(data_type):
    column = _json_column({"a.b": {"items": [[1, None, 3]]}})
    result = variant_get(column, '$["a.b"].items[0]', data_type)
    assert result.type == data_type
    assert result.to_pylist() == [[1, None, 3]]


def test_variant_get_nested_struct_and_map():
    data_type = pa.struct([
        ("flag", pa.bool_()),
        ("missing", pa.string()),
        ("items", pa.list_(pa.map_(pa.large_string(), pa.int16()))),
    ])
    column = _json_column({"obj": {"flag": True, "items": [{"b": 2, "a": 1}, None]}})
    result = variant_get(column, "$.obj", data_type)
    assert result.type == data_type
    assert result.to_pylist() == [
        {"flag": True, "missing": None, "items": [[("a", 1), ("b", 2)], None]}
    ]
    assert variant_get(column, "$.absent", data_type).to_pylist() == [None]
    assert variant_get(_json_column({}), "$", pa.struct([])).to_pylist() == [{}]


@pytest.mark.parametrize(
    "encoded,data_type",
    [
        (b"\x0c\x01", pa.float32()),
        (b"\x38" + struct.pack("<f", 1.25), pa.float64()),
        (b"\x1c" + struct.pack("<d", 1.25), pa.float32()),
        (b"\x05x", pa.int64()),
        (b"\x20\x02" + struct.pack("<i", 1230), pa.decimal128(4, 1)),
        (b"\x30" + struct.pack("<q", 0), pa.timestamp("us")),
    ],
)
def test_variant_get_rejects_implicit_casts(encoded, data_type):
    with pytest.raises(TypeError, match="does not match"):
        variant_get(_column([encoded]), "$", data_type)


def test_variant_get_boundaries_and_input_validation():
    with pytest.raises(ValueError, match="range|overflow|fit"):
        variant_get(_column([b"\x10\x80\x00"]), "$", pa.int8())
    with pytest.raises(ValueError, match="precision"):
        variant_get(_column([b"\x20\x02" + struct.pack("<i", 1230)]), "$", pa.decimal128(3, 2))
    with pytest.raises(TypeError, match="does not match"):
        variant_get(_json_column([1, 2]), "$", pa.list_(pa.int64(), 3))
    for target in (
        pa.uint32(), pa.timestamp("ms"), pa.map_(pa.int32(), pa.int64()),
        pa.list_(pa.uint32()),
    ):
        with pytest.raises(TypeError, match="Unsupported exact"):
            variant_get(_column([]), "$", target)
    with pytest.raises(ValueError, match="scale"):
        variant_get(_column([]), "$", pa.list_(pa.decimal128(4, -1)))
    with pytest.raises(ValueError, match="path"):
        variant_get(_column([]), "invalid", pa.int64())
    with pytest.raises(ValueError):
        variant_get(_column([b"\x02"]), "$", pa.int64())
    with pytest.raises(ValueError, match="StructArray"):
        variant_get(pa.array([1]), "$", pa.int64())
    with pytest.raises(TypeError, match="PyArrow data type"):
        variant_get(_column([]), "$", "int64")


def test_variant_get_dictionary_metadata_and_sliced_input():
    column = _column([b"\x0c\x01", b"\x0c\x02", None])
    dictionary = pa.StructArray.from_arrays(
        [column.field(0), column.field(1).dictionary_encode()],
        names=["value", "metadata"],
        mask=column.is_null(),
    )
    assert variant_get(dictionary.slice(1), "$", pa.int64()).to_pylist() == [2, None]
    assert variant_get(column.slice(0, 0), "$", pa.int64()).to_pylist() == []


@pytest.mark.parametrize("bits", [8, 16, 32, 64])
def test_variant_get_integer_bounds(bits):
    target = getattr(pa, f"int{bits}")()
    bounds = [-(2 ** (bits - 1)), 2 ** (bits - 1) - 1]
    column = _column([b"\x18" + struct.pack("<q", value) for value in bounds])
    assert variant_get(column, "$", target).to_pylist() == bounds
    if bits < 64:
        for value in (bounds[0] - 1, bounds[1] + 1):
            with pytest.raises(ValueError, match="range"):
                variant_get(_column([b"\x18" + struct.pack("<q", value)]), "$", target)


@pytest.mark.parametrize(
    "target",
    [
        pa.struct([]), pa.struct([("x", pa.int64())]), pa.list_(pa.int64()),
        pa.large_list(pa.int64()), pa.list_(pa.int64(), 0), pa.list_(pa.int64(), 2),
        pa.map_(pa.string(), pa.int64()),
    ],
)
def test_variant_get_complex_nulls_and_empty_batches(target):
    result = variant_get(_column([b"\x00", None]), "$", target)
    assert result.type == target
    assert result.to_pylist() == [None, None]
    assert variant_get(_column([]), "$", target).type == target
    assert variant_get(_column([]), "$", target).to_pylist() == []


def test_variant_get_rejects_null_metadata_dictionary_values():
    metadata = pa.DictionaryArray.from_arrays(
        pa.array([0], pa.int32()), pa.array([None], pa.binary()),
    )
    column = pa.StructArray.from_arrays(
        [pa.array([b"\x0c\x01"], pa.binary()), metadata], names=["value", "metadata"],
    )
    with pytest.raises(ValueError, match="null.*metadata"):
        variant_get(column, "$", pa.int64())


@pytest.mark.parametrize("value_type", [pa.binary(), pa.large_binary()])
@pytest.mark.parametrize("metadata_type", [pa.binary(), pa.large_binary()])
@pytest.mark.parametrize("dictionary", [False, True])
def test_variant_get_binary_storage_layouts(value_type, metadata_type, dictionary):
    metadata = pa.array([b"\x01\x00\x00"] * 3, metadata_type)
    if dictionary:
        metadata = metadata.dictionary_encode()
    column = pa.StructArray.from_arrays(
        [pa.array([b"\x0c\x01", b"\x0c\x02", None], value_type), metadata],
        names=["value", "metadata"], mask=pa.array([False, False, True]),
    )
    assert variant_get(column.slice(1), "$", pa.int64()).to_pylist() == [2, None]
    assert variant_get(column.slice(0, 0), "$", pa.int64()).to_pylist() == []


@pytest.mark.parametrize(
    "value,target,expected",
    [
        ({}, pa.struct([pa.field("x", pa.int64(), nullable=False)]), {"x": None}),
        ([1, None], pa.list_(pa.field("item", pa.int64(), nullable=False)), [1, None]),
        ([1, None], pa.large_list(pa.field("item", pa.int64(), nullable=False)), [1, None]),
        ([1, None], pa.list_(pa.field("item", pa.int64(), nullable=False), 2), [1, None]),
        ({"x": None}, pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=False)),
         [("x", None)]),
        (
            {"items": [None]},
            pa.struct([pa.field("items", pa.list_(pa.field("element", pa.int64(), False)),
                                False, metadata={b"source": b"variant"})]),
            {"items": [None]},
        ),
    ],
)
def test_variant_get_preserves_nested_field_annotations(value, target, expected):
    result = variant_get(_json_column(value), "$", target)
    assert result.type.equals(target, check_metadata=True)
    assert result.to_pylist() == [expected]
    result.validate(full=True)


@pytest.mark.parametrize("scale", [4, 39, 127, 128, 2147483647])
@pytest.mark.parametrize("nested", [False, True])
def test_variant_get_decimal_scale_above_precision_allows_empty_and_null(scale, nested):
    target = pa.decimal128(1, scale)
    if nested:
        target = pa.list_(target)
    assert variant_get(_column([]), "$", target).type == target
    result = variant_get(_column([b"\x00", None]), "$", target)
    assert result.type == target
    assert result.to_pylist() == [None, None]
    if not nested:
        with pytest.raises(TypeError, match="does not match"):
            variant_get(_column([b"\x20\x02" + struct.pack("<i", 1)]), "$", target)


@pytest.mark.parametrize(
    "target",
    [_LogicalIntType(), pa.list_(_LogicalIntType()), pa.struct([("x", _LogicalIntType())]),
     pa.map_(pa.string(), _LogicalIntType())],
)
def test_variant_get_rejects_extension_targets(target):
    with pytest.raises(TypeError, match="Unsupported exact"):
        variant_get(_column([]), "$", target)


def test_variant_get_decimal_precision_accounts_for_fractional_zeroes():
    encoded = b"\x20\x04" + struct.pack("<i", 1)
    with pytest.raises(ValueError, match="precision"):
        variant_get(_column([encoded]), "$", pa.decimal128(1, 4))
    assert variant_get(_column([encoded]), "$", pa.decimal128(4, 4)).to_pylist() == [Decimal("0.0001")]


@pytest.mark.parametrize("nested", [False, True])
def test_variant_get_preserves_opaque_metadata_and_custom_map_names(nested):
    metadata = {b"key\x00": b"\xff\x00"}
    target = pa.map_(
        pa.field("mykey", pa.large_string(), False, metadata=metadata),
        pa.field("myvalue", pa.int64(), False, metadata=metadata), True,
    )
    value, expected = {"x": None}, [("x", None)]
    if nested:
        target = pa.struct([
            pa.field("items", pa.list_(target), False, metadata=metadata),
            pa.field("missing", pa.int64(), False, metadata=metadata),
        ])
        value, expected = {"items": [value]}, {"items": [expected], "missing": None}
    result = variant_get(_json_column(value), "$", target)
    assert result.type.equals(target, check_metadata=True)
    assert result.to_pylist() == [expected]
    result.validate(full=True)


def test_variant_get_preserves_nul_in_struct_field_names():
    target = pa.struct([("a\x00b", pa.int64()), ("a", pa.int64())])
    result = variant_get(_json_column({"a\x00b": 42, "a": 7}), "$", target)
    assert result.type.equals(target, check_metadata=True)
    assert result.to_pylist() == [{"a\x00b": 42, "a": 7}]


def test_variant_get_input_annotations_do_not_change_storage_identity():
    metadata = {b"source": b"\xff\x00"}
    column = pa.StructArray.from_arrays(
        [pa.array([b"\x0c\x01", None], pa.binary()), pa.array([b"\x01\x00\x00"] * 2, pa.binary())],
        fields=[pa.field("value", pa.binary(), metadata=metadata),
                pa.field("metadata", pa.binary(), metadata=metadata)],
        mask=pa.array([False, True]),
    )
    assert variant_get(column, "$", pa.int64()).to_pylist() == [1, None]
    for names in (["value\x00suffix", "metadata"], ["value", "metadata\x00suffix"]):
        invalid = pa.StructArray.from_arrays(list(column.flatten()), names=names)
        with pytest.raises(ValueError, match="fields value and metadata"):
            variant_get(invalid, "$", pa.int64())


@pytest.mark.parametrize("binary_type", [pa.binary(), pa.large_binary()])
@pytest.mark.parametrize("metadata", [None, {b"source": b"\xff"}])
def test_variant_get_sliced_nonnullable_children(binary_type, metadata):
    values = pa.array([b"\x0c\x09", b"\x0c\x01", None, b"\x0c\x02"], binary_type).slice(1)
    names = pa.array([b"\x01\x00\x00"] * 2 + [None, b"\x01\x00\x00"], binary_type).slice(1)
    column = pa.StructArray.from_arrays(
        [values, names],
        fields=[pa.field("value", binary_type, False, metadata=metadata),
                pa.field("metadata", binary_type, False, metadata=metadata)],
        mask=pa.array([False, True, False]),
    )
    for offset, length, expected in [(0, 1, [1]), (1, 2, [None, 2]), (2, 1, [2]), (3, 0, [])]:
        sliced = column.slice(offset, length)
        sliced.validate(full=True)
        assert variant_get(sliced, "$", pa.int64()).to_pylist() == expected


def test_variant_get_numeric_fields_returns_ordered_fixed_size_lists():
    # Java-compatible encoding for {"age": 27, "city": "Beijing"}.
    value = bytes(
        [
            0x02,
            0x02,
            0x00,
            0x01,
            0x00,
            0x02,
            0x0A,
            0x0C,
            0x1B,
            0x1D,
            ord("B"),
            ord("e"),
            ord("i"),
            ord("j"),
            ord("i"),
            ord("n"),
            ord("g"),
        ]
    )
    metadata = bytes([0x01, 0x02, 0x00, 0x03, 0x07]) + b"agecity"
    column = pa.StructArray.from_arrays(
        [pa.array([value, b""], pa.binary()), pa.array([metadata, b""], pa.binary())],
        fields=[
            pa.field("value", pa.binary(), nullable=False),
            pa.field("metadata", pa.binary(), nullable=False),
        ],
        mask=pa.array([False, True]),
    )

    output = variant_get_numeric_fields(column, ["missing", "age"])

    assert output.type == pa.list_(pa.field("item", pa.float32()), 2)
    assert output.to_pylist() == [[None, 27.0], None]

    with pytest.raises(NotImplementedError, match="city"):
        variant_get_numeric_fields(column, ["city"])
    with pytest.raises(ValueError, match="must not be empty"):
        variant_get_numeric_fields(column, [])
