// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Read a primary-key vector table over a bucket split that JAVA PLANNED, the
//! shape an engine uses when planning runs in Paimon Java and execution is
//! shipped elsewhere.
//!
//! Both halves of the fixture come from ONE Java run, which is what makes this a
//! cross-language test rather than a round-trip of our own bytes: the table
//! directory was written by Java's `ivf-flat` indexer, and the split bytes are
//! what Java's `PrimaryKeyVectorScan` planned over that same table and
//! serialized through `BucketVectorSearchSplit.serialize`. The split names its
//! data and index files by their generated UUIDs, so bytes from a different run
//! would reference files that do not exist.
//!
//! Provenance of `testdata/pkvector_split` (opaque binary, regenerate rather
//! than hand-edit):
//!   * Source: Apache Paimon Java master `3e510cf132`.
//!   * Generator: `PkVectorSplitFixtureGenerator` (module `paimon-vector`).
//!   * Command: `mvn -pl paimon-vector test -Dtest=PkVectorSplitFixtureGenerator \
//!               -Dgen.pkvector.split.fixture=true -Dgen.pkvector.split.out=<dir>`.
//!   * Config: primary key `id`, vector column `embedding`, `ivf-flat`,
//!     `nlist = 1` (exact, deterministic single inverted list), `deduplicate`
//!     merge engine, deletion vectors enabled, one bucket. Compacted, since the
//!     ANN segment is only built at `level > 0`.
//!   * Rows: `id == row position`, vectors `[0,0] [1,0] [2,0] [3,0] [4,0]`.
//!   * Query `[0, 0]`, squared-L2 distances `[0, 1, 4, 9, 16]`; top-3 -> ids
//!     `[0, 1, 2]`, scores `1/(1+d) = [1.0, 0.5, 0.2]`.
//!
//! The split embeds its bucket directory as an ABSOLUTE path, because that is
//! what Java serializes and what a real engine ships. The fixture is therefore
//! staged into a temp dir and that one path rewritten, below.

// Gated off Windows for the whole file: the fixture is opened via a `file://` URL
// built from a tempdir path, which the fs lister cannot strip a Windows prefix
// from. Matches how `pk_vector_java_fixture_test`, `pk_vector_baseline_test` and
// `rest_catalog_test` gate their `file://` tempdir tests.
#![cfg(not(target_os = "windows"))]

use std::path::Path;

use arrow_array::{Array, Float32Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::table::{BucketVectorSearchSplit, SchemaManager, Table};

const FIXTURE: &str = "testdata/pkvector_split";
const TABLE_DIR: &str = "table";
const VECTOR_COLUMN: &str = "embedding";

/// The data file the fixture's ANN segment indexes, named by the split itself.
const DATA_FILE: &str = "data-d15b376f-823b-47ff-b13a-97b68d0c0885-0.parquet";

/// The bucket path the generator baked into the split bytes, as a `writeUTF`
/// string: a 2-byte big-endian length followed by the bytes.
const GENERATED_BUCKET_PATH: &str = "/tmp/pkvfixture/warehouse/default.db/pk_vector_split/bucket-0";

/// Stage the committed fixture into a private temp root and rewrite the one
/// absolute path the split carries, so the split points at the staged table.
/// Returns the temp dir (kept alive by the caller), the opened table, and the
/// split bytes.
async fn open_bucket_split_fixture() -> (tempfile::TempDir, Table, Vec<Vec<u8>>) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src = Path::new(manifest_dir).join(FIXTURE);
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dst = tmp.path().join(TABLE_DIR);
    copy_dir(&src.join(TABLE_DIR), &dst);

    let staged_bucket_path = format!("{}/bucket-0", dst.display());
    let mut splits = Vec::new();
    for entry in std::fs::read_dir(&src).expect("read fixture dir") {
        let path = entry.expect("fixture entry").path();
        if path.extension().is_some_and(|e| e == "bin") {
            let bytes = std::fs::read(&path).expect("read split bytes");
            splits.push(rewrite_bucket_path(
                &bytes,
                GENERATED_BUCKET_PATH,
                &staged_bucket_path,
            ));
        }
    }
    assert!(!splits.is_empty(), "fixture carries no split bytes");

    let location = format!("file://{}", dst.display());
    let file_io: FileIO = FileIOBuilder::new("file").build().expect("build fs FileIO");
    let schema = SchemaManager::new(file_io.clone(), location.clone())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("fixture table has no schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", "pk_vector_split"),
        location,
        (*schema).clone(),
        None,
    );
    (tmp, table, splits)
}

/// Replace one `writeUTF`-encoded string in place, rewriting its 2-byte
/// big-endian length prefix. Both strings are ASCII, so byte length is the
/// encoded length.
fn rewrite_bucket_path(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let mut needle = (from.len() as u16).to_be_bytes().to_vec();
    needle.extend_from_slice(from.as_bytes());
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .unwrap_or_else(|| {
            panic!("split bytes do not carry the generated bucket path '{from}'; regenerate the fixture and update GENERATED_BUCKET_PATH")
        });

    let mut out = Vec::with_capacity(bytes.len() + to.len());
    out.extend_from_slice(&bytes[..at]);
    out.extend_from_slice(&(to.len() as u16).to_be_bytes());
    out.extend_from_slice(to.as_bytes());
    out.extend_from_slice(&bytes[at + needle.len()..]);
    out
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

fn batch_i32(batches: &[RecordBatch], column: &str) -> Vec<i32> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of(column)
            .unwrap_or_else(|_| panic!("column '{column}' missing from output"));
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not int32"));
        out.extend((0..array.len()).map(|i| array.value(i)));
    }
    out
}

fn batch_f32(batches: &[RecordBatch], column: &str) -> Vec<f32> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch
            .schema()
            .index_of(column)
            .unwrap_or_else(|_| panic!("column '{column}' missing from output"));
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not float32"));
        out.extend((0..array.len()).map(|i| array.value(i)));
    }
    out
}

async fn read_over_splits(table: &Table, splits: &[Vec<u8>], limit: usize) -> Vec<RecordBatch> {
    let refs: Vec<&[u8]> = splits.iter().map(Vec::as_slice).collect();
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(limit)
        .with_projection(&["id"]);
    builder
        .execute_read_for_bucket_splits(&refs)
        .await
        .expect("bucket-split read over the Java fixture failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting read batches failed")
}

/// The read is driven entirely by the Java-planned split: no index manifest is
/// consulted, and the rows come back best-first with their scores.
#[tokio::test]
async fn reads_java_planned_bucket_split() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let batches = read_over_splits(&table, &splits, 3).await;

    assert_eq!(
        batch_i32(&batches, "id"),
        vec![0, 1, 2],
        "rows must be best-first: squared-L2 from [0,0] is id*id"
    );

    let scores = batch_f32(&batches, "__paimon_search_score");
    for (got, want) in scores.iter().zip(&[1.0f32, 0.5, 0.2]) {
        assert!(
            (got - want).abs() < 1e-4,
            "score diverges: got {got}, want {want}"
        );
    }
}

/// The split route and the manifest route are two ways to reach the same plan
/// over the same snapshot, so on a table whose splits cover every bucket they
/// must agree exactly. Necessary but NOT sufficient on its own -- the two routes
/// agreeing is also what a read that quietly ignored the split and re-planned
/// from the manifest would produce. `restricts_the_read_to_the_splits_row_ranges`
/// is the test that separates them.
#[tokio::test]
async fn agrees_with_the_manifest_route() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let from_splits = read_over_splits(&table, &splits, 3).await;

    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(3)
        .with_projection(&["id"]);
    let from_manifest = builder
        .execute_read()
        .await
        .expect("manifest-route read failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting manifest-route batches failed");

    assert_eq!(
        batch_i32(&from_splits, "id"),
        batch_i32(&from_manifest, "id"),
        "the split route must return the manifest route's rows"
    );
    assert_eq!(
        batch_f32(&from_splits, "__paimon_search_score"),
        batch_f32(&from_manifest, "__paimon_search_score"),
        "the split route must return the manifest route's scores"
    );
}

/// A limit below the number of matching rows is applied to the search, not to
/// the output alone.
#[tokio::test]
async fn honors_a_narrower_limit() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    assert_eq!(
        batch_i32(&read_over_splits(&table, &splits, 1).await, "id"),
        vec![0]
    );
}

/// No splits cannot pin a snapshot, so it is rejected rather than answered as an
/// empty read -- which would look identical to a query that matched nothing.
#[tokio::test]
async fn rejects_an_empty_split_list() {
    let (_tmp, table, _splits) = open_bucket_split_fixture().await;
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(3);
    let error = match builder.execute_read_for_bucket_splits(&[]).await {
        Ok(_) => panic!("an empty split list must be rejected"),
        Err(e) => e,
    };
    assert!(
        error.to_string().contains("at least one split"),
        "unexpected error: {error}"
    );
}

/// The bytes come from outside the process, so a corrupt buffer must fail as
/// invalid data rather than as an internal fault.
#[tokio::test]
async fn rejects_corrupt_split_bytes() {
    // A decoder assertion, not a read assertion. The entry point takes SERIALIZED
    // bytes and decodes them at that boundary, so corrupt input is refused there
    // and never reaches a search; driving the read as well would run the same
    // decoder behind a query that cannot execute either way.
    let (_tmp, _table, splits) = open_bucket_split_fixture().await;
    let mut corrupt = splits[0].clone();
    corrupt[0] ^= 0xFF; // break the PKVSPLIT magic
    assert!(BucketVectorSearchSplit::deserialize(&corrupt).is_err());
}

/// The split's per-file row ranges are the read's authority, and nothing else
/// carries them: the index manifest places no positional restriction. Narrowing
/// the range to the first two rows must drop `id = 2` from a top-3 -- a read that
/// re-planned from the manifest, or that treated the ranges as advisory, would
/// still return three rows.
///
/// This is what makes the suite able to tell the two routes apart, so it is also
/// the test that fails first if `plan_for_bucket_vector_splits` stops being
/// reached.
#[tokio::test]
async fn restricts_the_read_to_the_splits_row_ranges() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let narrowed: Vec<Vec<u8>> = splits
        .iter()
        .map(|bytes| with_row_range(bytes, DATA_FILE, 1))
        .collect();

    // Limit ABOVE the number of allowed rows on purpose: with a limit of 3 the
    // answer would be the same either way, and the test would pass without the
    // range being applied at all.
    let ids = batch_i32(&read_over_splits(&table, &narrowed, 5).await, "id");
    assert_eq!(
        ids,
        vec![0, 1],
        "only the rows the split allows may produce candidates"
    );

    // The unrestricted split over the same query returns every row, so the
    // difference above is the range and nothing else.
    assert_eq!(
        batch_i32(&read_over_splits(&table, &splits, 5).await, "id"),
        vec![0, 1, 2, 3, 4]
    );
}

/// Append a row-range entry restricting `file` to `0..=to`.
///
/// The fixture's own `rangeFileCount` is ZERO, which is not an oversight: Java
/// only records a range for a file its pre-filter narrowed, and this query has no
/// pre-filter. That asymmetry is exactly what the read side has to normalize --
/// an omitted file means the whole file, while an explicitly listed one means
/// only what it lists -- so the two cases are worth driving separately, and this
/// builds the listed one. The section is last in the byte form and empty here, so
/// appending is rewriting its count.
fn with_row_range(bytes: &[u8], file: &str, to: i64) -> Vec<u8> {
    let head = bytes.len() - 4;
    assert_eq!(
        i32::from_be_bytes(bytes[head..].try_into().unwrap()),
        0,
        "fixture split was expected to carry no row ranges; regenerate and revisit"
    );

    let mut out = Vec::with_capacity(bytes.len() + file.len() + 24);
    out.extend_from_slice(&bytes[..head]);
    out.extend_from_slice(&1i32.to_be_bytes()); // rangeFileCount
    out.extend_from_slice(&(file.len() as u16).to_be_bytes());
    out.extend_from_slice(file.as_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // rangeCount
    out.extend_from_slice(&0i64.to_be_bytes()); // from
    out.extend_from_slice(&to.to_be_bytes());
    out
}
