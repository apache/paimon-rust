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

from typing import BinaryIO, Optional, Sequence

import pyarrow

from .uri_reader import UriReaderFactory

class VideoFrameDescriptor:
    """Java/PyPaimon-compatible reference to one frame of a complete encoded video."""
    def __init__(self, uri: str, offset: int, length: int, frame_index: int,
                 moov_offset: int = 0, moov_length: int = 0) -> None: ...
    @property
    def uri(self) -> str: ...
    @property
    def offset(self) -> int: ...
    @property
    def length(self) -> int: ...
    @property
    def frame_index(self) -> int: ...
    @property
    def moov_offset(self) -> int: ...
    @property
    def moov_length(self) -> int: ...
    def serialize(self) -> bytes: ...
    @staticmethod
    def deserialize(value: bytes) -> "VideoFrameDescriptor": ...

class VideoFormatWriter:
    """Own an output stream and pack encoded videos without decoding them.

    Sources default to local files/HTTP. Supply a URI factory for PyPaimon FileIO.
    A failed write closes the stream; callers remove the incomplete destination.
    """
    def __init__(self, output_stream: BinaryIO, *,
                 uri_reader_factory: Optional[UriReaderFactory] = None,
                 copy_buffer_size: int = 4096) -> None: ...
    def write_frames(self, frames: Sequence[Optional[bytes]]) -> None:
        """Accept serialized VideoFrameDescriptors from Rust or PyPaimon, and NULLs."""
        ...
    def write_arrow(self, batch: pyarrow.RecordBatch) -> None:
        """Accept one large_binary column of frame descriptors."""
        ...
    def num_bytes(self) -> int: ...
    def close(self) -> int:
        """Finalize and close; return the total file size. Repeated close is safe."""
        ...
    def abort(self) -> None:
        """Close without finalizing. The caller removes the incomplete output."""
        ...

def read_arrow_schema(fields: str) -> pyarrow.Schema:
    """Build the runtime read schema from Paimon fields (JSON)."""
    ...

def normalize_array_for_write(column: pyarrow.Array, target_type: pyarrow.DataType) -> pyarrow.Array:
    """Normalize a write layout and validate visible NOT NULL children in core."""
    ...

def cast_array_for_schema_evolution(
    column: pyarrow.Array, source_type: str, target_type: str
) -> pyarrow.Array:
    """Cast historical Paimon types (JSON) using the core table-reader rules."""
    ...

def variant_get_numeric_fields(
    column: pyarrow.Array, fields: Sequence[str]
) -> pyarrow.FixedSizeListArray:
    """Extract literal top-level numeric fields as float32 in the requested order."""
    ...

def variant_get(
    column: pyarrow.Array, path: str, data_type: pyarrow.DataType
) -> pyarrow.Array:
    """Extract one VARIANT path without implicit casts.

    Supports bool, signed integers, float32/64, string/large_string,
    binary/large_binary, date32, timestamp('us'), decimal128 with nonnegative
    scale, and nested struct/list/large_list/fixed_size_list/string-keyed map.
    VARIANT value/metadata children accept binary or large_binary storage.
    Missing paths, VARIANT nulls and SQL nulls return nulls. Type mismatches
    raise TypeError; malformed input and numeric overflow raise ValueError.
    """
    ...
