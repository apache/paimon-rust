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

# Java centroid index fixture

The Java implementation in [apache/paimon#11350](https://github.com/apache/paimon/pull/11350)
wrote the router and shard files with the paimon-vector-index 0.7.0-SNAPSHOT JNI
trainer and writer, and wrapped the descriptors with `CentroidIndexMeta`. The
router training input is four two-dimensional L2
vectors, `[0, 0]`, `[1, 0]`, `[100, 0]`, and `[101, 0]`, with two outer
centers and one local IVF list in each shard. Native labels are 0 through 3.
The Rust test publishes the files at row range [2, 5] to verify label rebasing.

The Java `CentroidBitmapIndex.Writer` produced `bitmap.index` and
`CentroidIndexMeta.scalarIndex` wrapped its directory in `bitmap.meta`.
It contains a `tenant` section (field ID 0) with values `7` and `9` and a
`status` section (field ID 2) with values NULL and `1`. The two labels
are 2 and 4 relative to the common source range.
`source.meta` is Java's `DataEvolutionIndexSourceMeta` for snapshot 1 and schema 0.

## Table interchange

`warehouse/test.db` contains two Data Evolution tables written by the Spark
centroid fixture test in [apache/paimon#11350](https://github.com/apache/paimon/pull/11350).
`centroid_bitmap` has a single Bitmap field built at schema 1, a populated
range with an empty shard, and an all-null range. Spark builds this table
with U+00A0 around `l2`. `centroid_metrics` has L2 IVF-PQ with 64 local lists,
cosine IVF-FLAT, and inner product IVF-FLAT.

To regenerate the Java tables, run the Spark suite in the Java checkout
and copy the exported `test.db` into this directory's `warehouse`.
Set the exported schema files' `options.owner` to `paimon-test` for a
fixture independent of the Spark runner account.

```sh
PAIMON_JAVA_CENTROID_FIXTURE=/absolute/path/to/empty/warehouse \
  mvn -pl paimon-spark/paimon-spark-ut -am -Pspark3 -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.sql.CentroidVectorIndexTest -Dtest=none test
```

To regenerate the corresponding Rust tables, run the Rust fixture writer.
Zip the resulting `test.db` directory as `centroid-rust-warehouse.zip` at the
root of Java's `paimon-vector/src/test/resources`.

```sh
PAIMON_RUST_CENTROID_FIXTURE=/absolute/path/to/empty/warehouse \
  cargo test -p paimon --test centroid_java_interop_test \
  write_rust_centroid_warehouse -- --ignored --exact
```
