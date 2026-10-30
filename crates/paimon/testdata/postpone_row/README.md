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

# Java postpone ROW fixtures

Generated from Apache Paimon Java commit
`5be7e2dd1f722a093015e22332e2e220e1807324` with the included generator.
Both files contain the logical INSERT `(id=1, value=NULL)` with
`KeyValue.UNKNOWN_SEQUENCE=-1`.

- `java_postpone_kv_null.row`: full KV physical layout with duplicated `_KEY_id`.
- `java_postpone_thin_null.row`: thin physical layout without duplicated keys.

The full-layout fixture guards against inferring a ROW schema from successful
decoding: a NULL payload can make the wrong physical schema consume the entire
record. The writing schema and key-stat slots select the layout instead.

Regenerate after building the Java modules and bundle:

```sh
task_java_root=/path/to/paimon
task_fixture_out="$PWD"
task_fixture_classes="$(mktemp -d)"
task_fixture_cp="$task_java_root/paimon-format/target/classes:$task_java_root/paimon-core/target/classes:$task_java_root/paimon-common/target/classes:$task_java_root/paimon-api/target/classes:$task_java_root/paimon-bundle/target/paimon-bundle-2.2-SNAPSHOT.jar"
javac -cp "$task_fixture_cp" -d "$task_fixture_classes" \
  "$task_java_root/paimon-format/src/main/java/org/apache/paimon/format/row/RowFormatWriter.java" \
  "$task_java_root/paimon-format/src/main/java/org/apache/paimon/format/row/RowBlockWriter.java" \
  "$task_java_root/paimon-format/src/main/java/org/apache/paimon/format/row/BlockOutput.java" \
  "$task_java_root/paimon-format/src/main/java/org/apache/paimon/format/row/RowBlockIndex.java" \
  "$task_java_root/paimon-format/src/main/java/org/apache/paimon/format/row/RowFileFooter.java" \
  "$task_fixture_out/PostponeRowJavaFixture.java"
java -cp "$task_fixture_classes:$task_fixture_cp" PostponeRowJavaFixture "$task_fixture_out"
```

SHA-256:

```text
43a5756d3f8c1e3962e5a5756b9b3824e380f260ba3645437a4bb8f810f7b44a  java_postpone_kv_null.row
4b4156498bd4df17de5ccf7a448f06667ab3df28666dc9ad6c79f436f475212d  java_postpone_thin_null.row
```
