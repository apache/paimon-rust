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

# Java Vector compaction fixture

`java-vectors.parquet` is an unchanged Java-written Paimon Parquet file from
[apache/paimon-cpp, commit ee3c9592c311b43f4afffaafb5636ecf609f1bf3](https://github.com/apache/paimon-cpp/tree/ee3c9592c311b43f4afffaafb5636ecf609f1bf3/test/test_data/parquet/append_types_compatibility.db/java_vector_types).
Source: `bucket-0/data-325a3f55-53ea-4a75-81e0-0af91acbd76e-0.parquet`.

The source schema has `id` (field ID 0) and seven length-three Vector fields.
The test projects `f_vector_float` (field ID 6), whose two values are
`[1.25, -2.5, 3.75]` and `[4.25, 5.5, 6.75]`. It stages the original bytes as
`*.vector.parquet` with an explicit write-column projection, then compacts two
versions, checks the full output vectors, and applies a subsequent field update.
This exercises Java Parquet Vector encoding and historical field-ID resolution;
the dedicated manifest metadata is assembled by the Rust test.

Reproduce the fixture by copying the source file above without rewriting it.
SHA-256: `db25daea1f0a82b7ba5d95f7030014221ebbce2a22bc1a09fd1ff8ca363987eb`.
