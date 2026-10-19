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

"""Regenerate the fastavro fixtures, with one record per OCF block."""

import json
from pathlib import Path

import fastavro

fixture_dir = Path(__file__).resolve().parent
schema = json.loads((fixture_dir / 'records.avsc').read_text())
records = [
    {'id': -7, 'label': 'before-epoch', 'payload': bytes([0, 1, 255])},
    {'id': 0, 'label': '\u4e8c', 'payload': b''},
    {'id': (1 << 63) - 1, 'label': 'end' + 'x' * 96, 'payload': bytes(range(32))},
]
for codec in ['bzip2', 'xz']:
    with (fixture_dir / ('fastavro_' + codec + '.avro')).open('wb') as output:
        fastavro.writer(output, schema, records, codec=codec, sync_interval=1,
                        sync_marker=bytes(range(16)))
