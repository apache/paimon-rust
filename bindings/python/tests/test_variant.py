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

import pyarrow as pa
import pytest

from pypaimon_rust.data import variant_get_float32


def test_variant_get_float32_returns_ordered_fixed_size_lists():
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

    output = variant_get_float32(column, ["missing", "age"])

    assert output.type == pa.list_(pa.field("item", pa.float32()), 2)
    assert output.to_pylist() == [[None, 27.0], None]

    with pytest.raises(NotImplementedError, match="city"):
        variant_get_float32(column, ["city"])
    with pytest.raises(ValueError, match="must not be empty"):
        variant_get_float32(column, [])
