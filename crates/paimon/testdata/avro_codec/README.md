# Avro manifest codec fixtures

These four Avro Object Container Files use the same three records and schema
(`records.avsc`), with one record per block and sync marker bytes `00` through
`0f`. They exercise the `bzip2` and `xz` codec names used by Java Paimon and
PyPaimon manifests. The records cover a negative long, zero, `Long.MAX_VALUE`,
UTF-8 text, empty bytes, and non-text bytes.

Producers:

- `java_*.avro`: Apache Avro Java **1.11.4**, the version in the Paimon Java
  build, using `CodecFactory.fromString`. Generated with
  `GenerateCodecFixtures.java`, JDK 8, Commons Compress 1.26.2, and XZ 1.9.
- `fastavro_*.avro`: fastavro **1.12.2**, generated with `generate.py` using
  the ordinary `fastavro.writer` API used by PyPaimon.

The Rust tests decode every record, check block boundaries and encoded offsets,
and check damaged compressed blocks and container sync markers. The fixture
files are static, so running the Rust tests does not require Java or Python.

To regenerate the Python fixtures:

```sh
python3 crates/paimon/testdata/avro_codec/generate.py
```

To regenerate the Java fixtures, set `AVRO_CP` to a classpath containing the
Avro version and its dependencies above, plus Jackson and SLF4J, then run from
the repository root:

```sh
mkdir -p /tmp/paimon-avro-codec-fixtures
javac -source 8 -target 8 -encoding UTF-8 -cp "$AVRO_CP" \
  -d /tmp/paimon-avro-codec-fixtures \
  crates/paimon/testdata/avro_codec/GenerateCodecFixtures.java
java -cp "/tmp/paimon-avro-codec-fixtures:$AVRO_CP" \
  GenerateCodecFixtures crates/paimon/testdata/avro_codec
```
