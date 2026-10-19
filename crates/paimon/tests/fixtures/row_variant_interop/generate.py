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

"""Regenerate fixtures or check Rust output with actual Java/Python readers."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--paimon-repo', required=True, type=Path)
parser.add_argument('--generate', action='store_true')
parser.add_argument('--check-rust-output', type=Path)
args = parser.parse_args()
sys.path.insert(0, str(args.paimon_repo / 'paimon-python'))

import pyarrow as pa
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.read.reader.format_row_reader import FormatRowReader
from pypaimon.schema.data_types import AtomicType, DataField, RowType, PyarrowFieldParser
from pypaimon.write.writer.format_row_writer import FormatRowWriter

root = Path(__file__).parent
schema = json.loads((root / 'schema.json').read_text())
# Java's real key ID is used here; physical IDs are not embedded in ROW files.
fields = [DataField(1073741823, '_KEY_id', AtomicType('INT', False)),
          DataField(2147483646, '_SEQUENCE_NUMBER', AtomicType('BIGINT', False)),
          DataField(2147483645, '_VALUE_KIND', AtomicType('TINYINT', False))]
fields += RowType.from_dict({'type': 'ROW', 'fields': schema['fields']}).fields

class FileIO:
    def get_file_size(self, path):
        return Path(path).stat().st_size

    def new_input_stream(self, path):
        return open(path, 'rb')

def read(path):
    reader = FormatRowReader(FileIO(), str(path), [f.name for f in fields], fields, None)
    batches = []
    while (batch := reader.read_arrow_batch()) is not None:
        batches.append(batch)
    reader.close()
    return pa.Table.from_batches(batches)

def write(path, data):
    with path.open('wb') as stream:
        writer = FormatRowWriter(stream, fields)
        writer.write_table(data)
        writer.close()

def encoded(value):
    if value is None:
        return 'SQLNULL'
    return value['value'].hex() + ':' + value['metadata'].hex()

def rows(data):
    result = []
    for row in data.to_pylist():
        nested = row['nested']
        items = row['items']
        attrs = row['attrs']
        result.append('|'.join([
            'I' + str(row['_KEY_id']), 'L' + str(row['_SEQUENCE_NUMBER']),
            'B' + str(row['_VALUE_KIND']), 'I' + str(row['id']), encoded(row['payload']),
            'SQLNULL' if nested is None else 'R[' + encoded(nested['payload']) + ',S' + nested['note'] + ']',
            'SQLNULL' if items is None else 'A[' + ','.join(encoded(v) for v in items) + ']',
            'SQLNULL' if attrs is None else 'M[' + ','.join('S' + k + '=' + encoded(v) for k, v in attrs) + ']',
            encoded(row['extra'])]))
    return result

def digests(data):
    return [None if v is None else {
        'value': hashlib.sha256(v['value']).hexdigest(),
        'metadata': hashlib.sha256(v['metadata']).hexdigest()
    } for v in data.column('payload').to_pylist()]

report = args.paimon_repo / 'paimon-format/target/surefire-reports/TEST-org.apache.paimon.format.row.RowFormatReadWriteTest.xml'
properties = ET.parse(report).getroot().find('properties')
classpath = next(p.attrib['value'] for p in properties if p.attrib.get('name') == 'java.class.path')
with tempfile.TemporaryDirectory() as temp:
    subprocess.run(['javac', '-source', '8', '-target', '8', '-cp', classpath,
                    '-d', temp, str(root / 'CrossVariant.java')], check=True)

    def java(action, path):
        completed = subprocess.run(['java', '-cp', temp + ':' + classpath,
                                    'CrossVariant', action, str(path)],
                                   check=True, capture_output=True, text=True)
        return completed.stdout.splitlines()

    if args.generate:
        (root / 'java.row').unlink(missing_ok=True)
        expected = java('write', root / 'java.row')
        assert rows(read(root / 'java.row')) == expected
        values = [{'z': 1, 'a': {'c': [True, None, '中']}}, [1, {'x': 'value'}, None],
                  None, 'scalar', {}, [], True, 1234567890123456789,
                  {f'key_{i:03}': 'x' * 100 for i in range(130)}]
        variants = [GenericVariant.from_python(value) for value in values]
        variants = [{'value': v.value(), 'metadata': v.metadata()} for v in variants]
        data = []
        for i, variant in enumerate(variants):
            data.append({'_KEY_id': i, '_SEQUENCE_NUMBER': i, '_VALUE_KIND': 0, 'id': i,
                         'payload': variant, 'extra': variant,
                         'nested': None if i == 3 else {'payload': variant, 'note': 'ok'},
                         'items': None if i == 5 else [variant, None, variants[2]],
                         'attrs': None if i == 6 else [('a', variant), ('b', None), ('c', variants[2])]})
        data = pa.Table.from_pylist(data, schema=PyarrowFieldParser.from_paimon_schema(fields))
        write(root / 'python.row', data)
        assert java('read', root / 'python.row') == rows(data)
        for name in ['java', 'python']:
            (root / f'{name}.json').write_text(json.dumps(digests(read(root / f'{name}.row')), indent=2) + '\n')
        print('Regenerated real Java/Python fixtures and verified both readers.')

    if args.check_rust_output:
        for name in ['java', 'python']:
            actual = rows(read(args.check_rust_output / f'rust_{name}.row'))
            assert actual == java('read', args.check_rust_output / f'rust_{name}.row')
            expected = rows(read(root / f'{name}.row'))
            # Rust generates its own sequence numbers. All keys, kinds and
            # logical values must still match byte for byte.
            without_sequence = lambda records: [r.split('|')[:1] + r.split('|')[2:] for r in records]
            assert without_sequence(actual) == without_sequence(expected)
        print('Rust output matches Java and Python: keys, kinds, all Variant buffers and nulls.')
