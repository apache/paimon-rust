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

# Sketch / Bitmap Java ↔ Rust fixtures

Deterministic payloads for `table/aggregator/java_rust_compat.rs`.

Committed `compat_*.bin` goldens follow the same pattern as existing
`hll_java_*.bin` / `roaring64_java_*.bin` fixtures: generated once by Java,
checked in, and loaded via `include_bytes!`.

## Dependencies (do not commit jars)

Classpath is resolved from `pom.xml` with the same versions as
`paimon-common`:

| Artifact | Version | Purpose |
|---|---|---|
| `org.roaringbitmap:RoaringBitmap` | `1.2.1` | RBM32 / RBM64 wire format (Paimon `${roaringbitmap.version}`) |
| `org.apache.datasketches:datasketches-java` | `4.2.0` | HLL / Theta (Paimon `paimon-common` dependency) |
| `org.apache.datasketches:datasketches-memory` | transitive | Required by datasketches-java |

```bash
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
```

`VerifyRustSketchBitmapCompat` uses the same production library call sequences
as Paimon `HllSketchUtil`, `ThetaSketch`, `RoaringBitmap32`, and
`RoaringBitmap64`. It does **not** shade/link `paimon-common` itself (would
pull the full Java module graph); versions are pinned to match production.

## Regenerate Java goldens

```bash
CP=$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)
javac -cp "$CP" GenerateSketchBitmapCompat.java
java -cp ".:$CP" GenerateSketchBitmapCompat .
```

## JVM verification of Rust-written bytes

Rust tests write payloads under the process temp directory (not under
`goldens/`), then compile/run:

```bash
CP=$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)
javac -cp "$CP" VerifyRustSketchBitmapCompat.java
java -cp ".:$CP" VerifyRustSketchBitmapCompat . /path/to/temp/rust_written
```

Do **not** commit: `lib/`, `*.class`, `cp.txt`, or any runtime temp output.
