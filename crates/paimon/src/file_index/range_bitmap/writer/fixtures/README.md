<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Range Bitmap Java fixtures

`GenerateRangeBitmap.java` generates the full-payload SHA-256 digests in
`../tests.rs`. The reference is Apache Paimon commit `1d368b4a5`, JDK 8,
and RoaringBitmap 1.2.1. Cases cover empty/all-null/singleton indexes,
zero-sized chunks, fixed/variable dictionary boundaries, and multiple
Roaring containers.

Set `PAIMON_CLASSPATH` to the reference checkout's compiled `paimon-common`
and `paimon-api` classes plus RoaringBitmap 1.2.1, jsr305 3.0.2 and slf4j-api
jars. From this directory:

```sh
fixture_classes=$(mktemp -d)
javac -cp "$PAIMON_CLASSPATH" -d "$fixture_classes" GenerateRangeBitmap.java
java -cp "$fixture_classes:$PAIMON_CLASSPATH" GenerateRangeBitmap
```

The Rust tests compare exact bytes for the small INT/STRING/FLOAT samples
in `range_bitmap.rs`; digests keep the larger boundary fixtures compact.
