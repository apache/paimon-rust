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

"""Source stream contracts for managed Blob URI reads."""

from typing import BinaryIO, Protocol

__all__ = ["UriReader", "UriReaderFactory"]


class UriReader(Protocol):
    """Open readable binary streams; Rust closes each stream after use."""

    def new_input_stream(self, uri: str) -> BinaryIO: ...


class UriReaderFactory(Protocol):
    """Select a URI reader while leaving payload copying to Rust Core."""

    def create(self, uri: str) -> UriReader: ...
