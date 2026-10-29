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

# Fixture generation only; Rust tests consume the saved file without Python.
from pathlib import Path
import pyarrow as pa
import pyarrow.orc as orc

leaf = pa.struct([('keep', pa.int32()), ('tail', pa.int32()), ('unused', pa.string())])
schema = pa.schema([
    ('profile', pa.struct([('detail', leaf)])),
    ('items', pa.list_(leaf)),
    ('groups', pa.list_(pa.list_(leaf))),
    ('attrs', pa.map_(pa.string(), leaf)),
    ('complex', pa.struct([('lookup', pa.map_(pa.string(), pa.list_(leaf)))])),
    ('array_maps', pa.list_(pa.map_(pa.string(), leaf))),
])
x = {'keep': 7, 'tail': 9, 'unused': 'discard'}
rows = [
    {'profile': {'detail': x}, 'items': [x, None, x], 'groups': [[x, None], [], None],
     'attrs': [('a', x), ('b', None)], 'complex': {'lookup': [('x', [x, None]), ('y', [])]},
     'array_maps': [[('k', x)], None, []]},
    {'profile': None, 'items': None, 'groups': None, 'attrs': None, 'complex': None, 'array_maps': None},
    {'profile': {'detail': None}, 'items': [], 'groups': [], 'attrs': [], 'complex': {'lookup': []}, 'array_maps': []},
    {'profile': {'detail': {'keep': None, 'tail': 3}}, 'items': [{'keep': None, 'tail': 4}], 'groups': [[None]],
     'attrs': [('z', {'keep': None, 'tail': 5})], 'complex': {'lookup': None}, 'array_maps': [[('z', None)]]},
    {'profile': {'detail': {'unused': 'present'}}, 'items': [{'unused': 'present'}], 'groups': [[{'unused': 'present'}]],
     'attrs': [('k', {'unused': 'present'})], 'complex': {'lookup': [('k', [{'unused': 'present'}])]},
     'array_maps': [[('k', {'unused': 'present'})]]},
]
orc.write_table(pa.Table.from_pylist(rows, schema=schema), Path(__file__).with_name('nested.orc'), compression='uncompressed')
