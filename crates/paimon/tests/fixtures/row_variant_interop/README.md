<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ROW v1 Variant interoperability fixtures

These files were written with Apache Paimon Java and Python from PR #10650
(`e4aa5dc9a5cc3c9e8bb7ff48df7b521e036d9eda`), then read and rewritten by Rust.
Both actual Java and Python readers checked the Rust output. No Rust encoder
was used to construct the input fixtures.

`java.row` is written by `CrossVariant.java` using `FileFormat`'s ROW writer.
`python.row` is written by `generate.py` using Python's `FormatRowWriter`.
The two fixtures deliberately use different Variant object builders and values.
Their logical table schema is `schema.json`; their physical PK layout is:

```text
_KEY_id, _SEQUENCE_NUMBER, _VALUE_KIND, id, payload, nested, items, attrs, extra
```

The key ID comes from Java's `SpecialFields.KEY_FIELD_ID_START` (1073741823),
with sequence/kind IDs 2147483646/2147483645. ROW v1 stores fields positionally
and does not embed field IDs. Nine columns exercise a null bitmap crossing a
byte boundary. The block uses ZSTD and the footer retains version 1.

The nine rows cover objects, arrays, JSON null, SQL null (Java), strings, empty
objects/arrays, booleans, a large integer, Unicode, and a Python object with
130 keys and long strings. Every Variant is also repeated under ROW, ARRAY,
and MAP, with SQL-null containers/elements and JSON-null elements. The small
`java.json`/`python.json` files record SHA256 of each root Variant's original
value and metadata buffers. Repeated children must have the same buffers;
this avoids storing large repeated hex dumps. SQL null is represented by a
null digest record, whereas JSON null has two concrete buffer digests.

The Rust test reads the canonical PK layout, checks all buffer digests and
container validity, writes through Rust's native PK writer, commits, and
reads the resulting table again. Run it without a Java/Python dependency:

```sh
cargo test -p paimon --test row_variant_interop_test --locked
```

To regenerate or cross-check with an adjacent Paimon checkout, first compile
and run `RowFormatReadWriteTest` there so its Surefire report supplies the
resolved Java classpath. Python needs the checkout's normal PyArrow/ZSTD
requirements, and `javac`/`java` must be installed:

```sh
mvn -pl paimon-format -am -DwildcardSuites=none \
  -Dtest=RowFormatReadWriteTest -DfailIfNoTests=false test
```

From the Rust checkout:

```sh
python3 crates/paimon/tests/fixtures/row_variant_interop/generate.py \
  --paimon-repo /path/to/paimon --generate

PAIMON_ROW_INTEROP_OUTPUT=/tmp/paimon-row-interop \
  cargo test -p paimon --test row_variant_interop_test --locked
python3 crates/paimon/tests/fixtures/row_variant_interop/generate.py \
  --paimon-repo /path/to/paimon --check-rust-output /tmp/paimon-row-interop
```

The external check reads Rust's exported files independently with Java and
Python and compares keys, row kinds, Variant buffers, and null containers.
Rust's generated sequence numbers are checked by those readers but are not
required to equal the sequence numbers in the input fixtures. Regenerating
with another ZSTD version can change compressed bytes without changing the
ROW payload or the recorded Variant digests.
