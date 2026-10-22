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

"""The package bootstrap exports stream protocols without an SDK rebuild."""

from pathlib import Path
import subprocess
import sys


def test_public_uri_protocols_are_runtime_imports():
    source = Path(__file__).resolve().parents[1] / 'python'
    code = r"""
import io
import sys
import types

# Model the dynamic submodule created by the extension. Load the real Python
# package bootstrap and protocol source without loading an installed extension.
engine = types.ModuleType('pypaimon_rust.pypaimon_rust')
engine.datafusion = types.ModuleType('pypaimon_rust.datafusion')
engine.__all__ = ['datafusion']
sys.modules[engine.__name__] = engine
sys.modules[engine.datafusion.__name__] = engine.datafusion
sys.path.insert(0, sys.argv[1])

from pypaimon_rust import UriReader as TopReader, UriReaderFactory as TopFactory
from pypaimon_rust.datafusion import UriReader, UriReaderFactory
from pypaimon_rust.uri_reader import UriReader as SourceReader
assert UriReader is TopReader is SourceReader
assert UriReaderFactory is TopFactory

class Reader(UriReader):
    def new_input_stream(self, uri):
        return io.BytesIO(uri.encode())

class Factory(UriReaderFactory):
    def create(self, uri) -> UriReader:
        return Reader()

with Factory().create('custom://source').new_input_stream('custom://source') as stream:
    assert stream.read() == b'custom://source'

assert not hasattr(engine.datafusion, '_UriReader')
assert not hasattr(engine.datafusion, '_UriReaderFactory')
"""
    subprocess.run([sys.executable, '-c', code, str(source)], check=True)
