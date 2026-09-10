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
use paimon::spec::{Datum, PredicateBuilder};
use paimon::table::{BucketVectorSearchSplit, PkVectorIndexedSplit, SchemaManager, Table};

const FIXTURE: &str = "testdata/pkvector_split";
const TABLE_DIR: &str = "table";
/// The snapshot the fixture's Java run committed, read from the fixture itself so
/// the assertion cannot drift from it.
fn fixture_snapshot_id() -> i64 {
    include_str!("../testdata/pkvector_split/snapshot_id.txt")
        .trim()
        .parse()
        .expect("fixture snapshot_id.txt must hold a snapshot id")
}

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

fn decode_bucket_splits(splits: &[Vec<u8>]) -> Vec<BucketVectorSearchSplit> {
    splits
        .iter()
        .map(|bytes| BucketVectorSearchSplit::deserialize(bytes))
        .collect::<paimon::Result<Vec<_>>>()
        .expect("Java bucket splits must decode")
}

/// Step one: search the Java-planned splits. Note there is no projection here --
/// what the search returns is positions and scores, not columns.
async fn search_over_splits(
    table: &Table,
    splits: &[Vec<u8>],
    limit: usize,
) -> Vec<PkVectorIndexedSplit> {
    let splits = decode_bucket_splits(splits);
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![0.0, 0.0])
        .with_limit(limit);
    builder
        .new_vector_read()
        .expect("vector read must build")
        .read(splits)
        .await
        .expect("bucket-split search over the Java fixture failed")
}

/// Step two: an ORDINARY read, driven by the caller's own read builder. This is
/// the half the engine owns.
async fn read_result(
    table: &Table,
    splits: &[PkVectorIndexedSplit],
    columns: &[&str],
) -> Vec<RecordBatch> {
    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(columns)
        .expect("projection must resolve");
    read_builder
        .new_read()
        .expect("new_read must succeed")
        .to_arrow_indexed(splits)
        .expect("vector search read failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting read batches failed")
}

/// Both steps, for the tests that only care about the rows.
async fn search_and_read(table: &Table, splits: &[Vec<u8>], limit: usize) -> Vec<RecordBatch> {
    let selected = search_over_splits(table, splits, limit).await;
    read_result(table, &selected, &["id"]).await
}

/// How many rows the search selected, across every file it selected from.
fn selected_rows(splits: &[PkVectorIndexedSplit]) -> usize {
    splits
        .iter()
        .flat_map(|s| s.row_ranges())
        .map(|r| r.count() as usize)
        .sum()
}

/// The scores the search attached, in the same (file, position) order the read emits.
fn selected_scores(splits: &[PkVectorIndexedSplit]) -> Vec<f32> {
    splits
        .iter()
        .flat_map(|s| s.scores().unwrap_or_default().to_vec())
        .collect()
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

/// What the search decided, ASSERTED BEFORE ANY DATA FILE IS OPENED -- the property
/// the one-shot terminal cannot offer, and the reason this route exists. A caller
/// distributing work learns how many files matched, which rows of them, and at what
/// scores, and can decide not to read at all.
///
/// Phase two then reads those same splits and checks the rows line up with what phase
/// one reported, which is what makes the two halves one search rather than two.
#[tokio::test]
async fn a_search_is_inspectable_before_its_rows_are_read() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;

    // Phase one: what the search selected, with no read at all.
    let selected = search_over_splits(&table, &splits, 3).await;
    assert_eq!(selected.len(), 1, "one split per data file selected from");
    assert_eq!(
        selected[0].data_split().data_files()[0].file_name,
        DATA_FILE
    );
    assert_eq!(selected[0].data_split().bucket(), 0);
    assert_eq!(selected_rows(&selected), 3);
    assert_eq!(
        selected[0].data_split().snapshot_id(),
        fixture_snapshot_id(),
        "the splits carry the snapshot the message named, not one re-resolved from the table"
    );
    assert!(
        !selected[0].data_split().raw_convertible(),
        "a derived primary-key split must not be readable raw, as in Java \
         PrimaryKeyScoredResult: a raw read would skip the merge"
    );
    // Query [0,0], so rank order and physical order coincide here.
    for (got, want) in selected_scores(&selected).iter().zip(&[1.0f32, 0.5, 0.2]) {
        assert!(
            (got - want).abs() < 1e-4,
            "search score diverges: got {got}, want {want}"
        );
    }

    // Phase two: the rows, through an ordinary read.
    let batches = read_result(&table, &selected, &["id"]).await;
    assert_eq!(batch_i32(&batches, "id"), vec![0, 1, 2]);
    assert_eq!(
        batch_f32(&batches, "__paimon_search_score"),
        selected_scores(&selected),
        "the read's score column must be the search's scores, in the same order"
    );
}

/// The two-step route and the one-shot terminal reach the same plan over the same
/// snapshot, so they must select the same rows. This is what pins the staged API as an
/// alternative SPELLING of the existing read rather than a second implementation of it.
#[tokio::test]
async fn the_two_step_route_agrees_with_the_one_shot_terminal() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let staged = search_and_read(&table, &splits, 3).await;
    let one_shot = read_over_splits(&table, &splits, 3).await;

    let mut staged_ids = batch_i32(&staged, "id");
    let mut one_shot_ids = batch_i32(&one_shot, "id");
    staged_ids.sort_unstable();
    one_shot_ids.sort_unstable();
    assert_eq!(
        staged_ids, one_shot_ids,
        "the same search selected different rows through the two terminals"
    );
}

/// The projection is the READ's business, not the search's: one search, read three
/// ways. The collapsed entry point could project too -- it forwarded the vector
/// builder's projection -- but only once per search, and only through that builder.
/// Reusing ONE search under several read projections is the new part.
#[tokio::test]
async fn read_projection_comes_from_the_read_builder() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let selected = search_over_splits(&table, &splits, 3).await;

    let one = read_result(&table, &selected, &["id"]).await;
    assert_eq!(
        column_names(&one),
        vec!["id", "__paimon_search_score"],
        "the score column is appended to whatever the caller projected"
    );

    let two = read_result(&table, &selected, &[VECTOR_COLUMN, "id"]).await;
    assert_eq!(
        column_names(&two),
        vec![VECTOR_COLUMN, "id", "__paimon_search_score"],
        "column ORDER is the caller's too"
    );

    // Same rows every time: the projection changes the columns, never the selection.
    assert_eq!(batch_i32(&one, "id"), batch_i32(&two, "id"));
    assert_eq!(
        batch_f32(&one, "__paimon_search_score"),
        batch_f32(&two, "__paimon_search_score")
    );
}

/// `ReadBuilder::with_projection` permits `_ROW_ID`, but the vector search read
/// recovers physical positions through it, so it cannot also be handed out as a
/// column. Rejected when the read is created, not deep inside the stream.
#[tokio::test]
async fn read_rejects_a_reserved_projection() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let selected = search_over_splits(&table, &splits, 3).await;

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&["_ROW_ID"])
        .expect("the read builder itself permits _ROW_ID");
    let error = read_builder
        .new_read()
        .expect("new_read must succeed")
        .to_arrow_indexed(&selected)
        .map(|_| ())
        .expect_err("_ROW_ID must not be readable as a column here");
    assert!(
        error.to_string().contains("_ROW_ID"),
        "unexpected error: {error}"
    );
}

/// A filter on the READ would be applied after Top-K, silently returning fewer
/// rows than the limit. It belongs on the search, which applies it before Top-K, so
/// this fails loud rather than quietly ignoring the caller.
#[tokio::test]
async fn read_rejects_a_filter_on_the_read_builder() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let selected = search_over_splits(&table, &splits, 3).await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["id"]).expect("projection");
    read_builder.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(0))
            .expect("build a filter on id"),
    );
    let error = read_builder
        .new_read()
        .expect("new_read must succeed")
        .to_arrow_indexed(&selected)
        .map(|_| ())
        .expect_err("a post-search filter must be rejected");
    assert!(
        error.to_string().contains("with_filter"),
        "the error must point at where the filter belongs: {error}"
    );
}

/// The column names of the first batch, for projection assertions.
fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .expect("at least one batch")
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Rows come back in PHYSICAL order, carrying their scores -- the read does not rank
/// them, as Java's does not either.
///
/// Every other test queries `[0,0]`, whose rank order happens to equal ascending
/// physical order, so none of them can tell the two apart. Querying nearest `[4,0]`
/// inverts them: the best match is the LAST row of the file. If the read ever started
/// ranking, this is the test that would catch it -- and the scores must land on the
/// right rows either way.
#[tokio::test]
async fn rows_come_back_in_physical_order_carrying_their_scores() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let splits = decode_bucket_splits(&splits);
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(vec![4.0, 0.0])
        .with_limit(3);
    let selected = builder
        .new_vector_read()
        .expect("vector read must build")
        .read(splits)
        .await
        .expect("search near [4,0]");

    // Vectors are [0,0] [1,0] .. [4,0], so squared-L2 from [4,0] is (4-id)^2 and the
    // top 3 are ids 4, 3, 2 -- which the split lists in ascending position order.
    assert_eq!(
        selected
            .iter()
            .flat_map(|s| s.row_ranges().iter())
            .flat_map(|r| r.from()..=r.to())
            .collect::<Vec<_>>(),
        vec![2, 3, 4],
        "the split lists positions ascending, not best-first"
    );

    let batches = read_result(&table, &selected, &["id"]).await;
    assert_eq!(
        batch_i32(&batches, "id"),
        vec![2, 3, 4],
        "the read emits physical order; ranking is the caller's, on the score column"
    );
    // 1/(1+d): id2 -> 1/5, id3 -> 1/2, id4 -> 1/1. Ascending here BECAUSE the rows are
    // in physical order -- the scores follow their own rows, not the ranking.
    for (got, want) in batch_f32(&batches, "__paimon_search_score")
        .iter()
        .zip(&[0.2f32, 0.5, 1.0])
    {
        assert!((got - want).abs() < 1e-4, "score {got} != {want}");
    }
}

/// A search that matched nothing selects no splits, and reads as an empty stream
/// rather than erroring.
#[tokio::test]
async fn an_empty_result_reads_as_no_rows() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    // Restrict every file to an empty range: the split says "no rows of this file".
    let excluded: Vec<Vec<u8>> = splits
        .iter()
        .map(|bytes| with_empty_row_range(bytes, DATA_FILE))
        .collect();
    let selected = search_over_splits(&table, &excluded, 3).await;

    assert!(
        selected.is_empty(),
        "nothing selected means no splits at all"
    );
    assert!(
        read_result(&table, &selected, &["id"]).await.is_empty(),
        "an empty selection reads as no batches, not an error"
    );
}

/// The projection belongs to the read, so one set on the SEARCH builder would be
/// dropped on the floor. Dropping it silently is the failure this PR refuses elsewhere
/// (a row filter factory is rejected for exactly that reason), so it is refused here
/// too -- including the "project nothing" form, which is still an instruction.
#[tokio::test]
async fn the_search_rejects_a_projection_that_belongs_to_the_read() {
    let (_tmp, table, _splits) = open_bucket_split_fixture().await;

    for columns in [vec!["id"], Vec::new()] {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(VECTOR_COLUMN)
            .with_query_vector(vec![0.0, 0.0])
            .with_limit(3)
            .with_projection(&columns);
        let error = match builder.new_vector_read() {
            Ok(_) => panic!("a projection on the search must be refused, not dropped"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("with_projection"),
            "the error must name the setter it refuses: {error}"
        );
    }
}

/// `read_type()` describes `to_arrow`, not this read, and a search that matched nothing
/// hands back no batch to learn the schema from -- so the indexed output schema has to
/// be askable before reading. Asserted against the real batches AND against the empty
/// case, because only the empty case proves it is not just reading the first batch.
#[tokio::test]
async fn indexed_read_type_matches_the_rows_that_come_back() {
    let (_tmp, table, splits) = open_bucket_split_fixture().await;
    let selected = search_over_splits(&table, &splits, 3).await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["id"]).expect("projection");
    let read = read_builder.new_read().expect("new_read must succeed");

    let declared: Vec<String> = read
        .indexed_read_type()
        .expect("the indexed schema must resolve")
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(declared, vec!["id", "__paimon_search_score"]);
    let batches = read_result(&table, &selected, &["id"]).await;
    assert_eq!(
        column_names(&batches),
        declared,
        "what is declared must be what is produced"
    );

    // Names are not enough: the score column must also carry the SAME Paimon field id
    // the declared schema gives it, or a caller converting `indexed_read_type()` to
    // Arrow gets a schema that differs from the batches in metadata alone.
    let declared_score = read
        .indexed_read_type()
        .unwrap()
        .last()
        .cloned()
        .expect("the score field");
    let produced_score = batches[0]
        .schema()
        .field_with_name("__paimon_search_score")
        .expect("the score column")
        .clone();
    assert_eq!(produced_score.data_type(), &arrow_schema::DataType::Float32);
    assert!(!produced_score.is_nullable());
    assert_eq!(
        produced_score.metadata().get("PARQUET:field_id"),
        Some(&declared_score.id().to_string()),
        "the produced column must carry the declared field id"
    );

    // Nothing matched: no batch exists, and the schema is still answerable.
    let excluded: Vec<Vec<u8>> = splits
        .iter()
        .map(|bytes| with_empty_row_range(bytes, DATA_FILE))
        .collect();
    let none = search_over_splits(&table, &excluded, 3).await;
    assert!(none.is_empty());
    assert!(read_result(&table, &none, &["id"]).await.is_empty());
    assert_eq!(
        read.indexed_read_type()
            .expect("still resolvable with no rows")
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>(),
        declared
    );
}

/// Append a row-range entry listing `file` with an EMPTY range list -- the split's
/// way of saying "no rows of this file", as distinct from omitting it entirely.
fn with_empty_row_range(bytes: &[u8], file: &str) -> Vec<u8> {
    let head = bytes.len() - 4;
    assert_eq!(
        i32::from_be_bytes(bytes[head..].try_into().unwrap()),
        0,
        "fixture split was expected to carry no row ranges; regenerate and revisit"
    );
    let mut out = Vec::with_capacity(bytes.len() + file.len() + 8);
    out.extend_from_slice(&bytes[..head]);
    out.extend_from_slice(&1i32.to_be_bytes()); // rangeFileCount
    out.extend_from_slice(&(file.len() as u16).to_be_bytes());
    out.extend_from_slice(file.as_bytes());
    out.extend_from_slice(&0i32.to_be_bytes()); // rangeCount = 0
    out
}
