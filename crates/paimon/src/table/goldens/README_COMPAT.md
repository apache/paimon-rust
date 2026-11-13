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
