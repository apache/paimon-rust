# Java FileIndex table fixtures

These four committed tables were produced through Apache Paimon Java's normal
catalog, append writer, commit, snapshot, and manifest paths. No index bytes or
metadata were assembled or edited by hand.

Provenance:

- Apache Paimon Java commit: `1d368b4a5932f8221fd28e2555001abdb8fb12ee`
  (`2.2-SNAPSHOT`).
- Generator: `java/JavaFileIndexTableFixtureGeneratorTest.java`.
- Tables: Bitmap and Bloom Filter, each with an embedded (`1 MB` threshold) and
  `.index` sidecar (`1 B` threshold) form.
- Common options: `bucket=1`, `bucket-key=id`, `file.format=parquet`, and
  `file-index.read.enabled=true`.
- Bloom options: `items=16`, `fpp=0.01`.
- Rows, in physical order: `(1, "keep")`, `(1, "drop")`,
  `(NULL, "null-id")`, `(3, "three")`.

The Rust test expects `id = 1` to return both duplicate rows, `id IS NULL` to
return the null row, `id = 1 AND payload = 'keep'` to retain only the residual
match, and `id = 2` to return no rows. The last predicate lies inside the data
file's `[1, 3]` min/max range, so manifest statistics cannot prune it; it is also
the observable FileIndex skip case.

To regenerate into an empty temporary directory:

```bash
git clone https://github.com/apache/paimon.git /tmp/paimon-java
git -C /tmp/paimon-java checkout 1d368b4a5932f8221fd28e2555001abdb8fb12ee
crates/paimon/testdata/file_index/regenerate.sh \
  /tmp/paimon-java /tmp/paimon-file-index-fixtures
```

After reviewing the generated metadata and running the Rust compatibility test,
replace `default.db/` with the generated `default.db/` directory. UUID-bearing
file names may change between runs; the rows, table options, storage shape, and
query expectations are deterministic.
