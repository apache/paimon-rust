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

"""Generate ORC physical MAP rows with Java/PyPaimon's Arrow footer metadata.

Requires pyarrow. Its ORC writer cannot set user metadata, so append the
ARROW:schema entry to the uncompressed protobuf footer and update its length.
The physical rows and field annotations follow MapSharedShreddingWritePlan.
"""

import base64
from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def read_varint(data, offset):
    value = shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
        shift += 7


def protobuf_fields(data):
    offset = 0
    while offset < len(data):
        start = offset
        tag, offset = read_varint(data, offset)
        if tag & 7 == 0:
            value, offset = read_varint(data, offset)
        elif tag & 7 == 2:
            size, offset = read_varint(data, offset)
            value = data[offset:offset + size]
            offset += size
        else:
            raise ValueError(f"Unexpected protobuf wire type: {tag & 7}")
        yield tag >> 3, value, data[start:offset]


def binary_field(number, data):
    return varint(number << 3 | 2) + varint(len(data)) + data


metadata = {
    "paimon.map.storage-layout": "shared-shredding",
    "paimon.map.shared-shredding.version": "1",
    "paimon.map.shared-shredding.field-dict": '{"a":0,"b":1}',
    "paimon.map.shared-shredding.field-dict-compression": "none",
    "paimon.map.shared-shredding.field-dict-original-size": "13",
    "paimon.map.shared-shredding.field-columns": '{"0":[0],"1":[0]}',
    "paimon.map.shared-shredding.overflow-set": "[0,1]",
    "paimon.map.shared-shredding.num-columns": "1",
    "paimon.map.shared-shredding.max-row-width": "2",
}
physical = pa.struct([
    pa.field("__field_mapping", pa.list_(pa.int32())),
    pa.field("__col_0", pa.int32()),
    pa.field("__overflow", pa.map_(pa.int32(), pa.int32())),
])
schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("tags", physical, metadata=metadata),
])
rows = [
    {"id": 0, "tags": {"__field_mapping": [0], "__col_0": 10, "__overflow": [(1, 20)]}},
    {"id": 1, "tags": None},
    {"id": 2, "tags": {"__field_mapping": [-1], "__col_0": None, "__overflow": []}},
    {"id": 3, "tags": {"__field_mapping": [1], "__col_0": None, "__overflow": [(0, 30)]}},
]
sink = pa.BufferOutputStream()
orc.write_table(pa.Table.from_pylist(rows, schema), sink, compression="uncompressed")
data = sink.getvalue().to_pybytes()
postscript = data[-1 - data[-1]:-1]
footer_length = next(value for number, value, _ in protobuf_fields(postscript) if number == 1)
footer_start = len(data) - 1 - data[-1] - footer_length
encoded_schema = base64.b64encode(schema.serialize().to_pybytes())
entry = binary_field(1, b"ARROW:schema") + binary_field(2, encoded_schema)
footer = data[footer_start:-1 - data[-1]] + binary_field(5, entry)
postscript = b"".join(
    varint(8) + varint(len(footer)) if number == 1 else raw
    for number, _, raw in protobuf_fields(postscript)
)
Path(__file__).with_name("shared.orc").write_bytes(
    data[:footer_start] + footer + postscript + bytes([len(postscript)])
)
