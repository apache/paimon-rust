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

//! Cross-language read-back of a primary-key vector table WRITTEN BY JAVA.
//!
//! Unlike `pk_vector_baseline_test` (which builds its table entirely on the Rust
//! write path), this test opens a table directory produced by Apache Paimon's
//! Java writer with the production `ivf-flat` primary-key vector indexer, and
//! asserts Rust reads + searches it correctly. It validates the cross-language
//! contract at the table-metadata layer (snapshot / manifest / data-file
//! `level`+`file_source` / `GlobalIndexMeta` / the `PrimaryKeyIndexSourceMeta` frame);
//! the ANN segment interior is the same native core on both sides
//! (`paimon-vindex-core` <-> its JNI wrapper), so a divergence here would be a
//! real metadata-parse bug, not a fixture artifact.
//!
//! Physical-position contract: Java never writes `first_row_id` on a primary-key
//! table (row-tracking is forbidden for PK tables), so the committed data files
//! below carry NO `first_row_id`. Row identity is therefore physical
//! `(file, position)`. Search returns those positions with scores; a projected
//! result read must retrieve the same rows without inventing global row IDs.

use std::path::Path;

use arrow_array::{Array, Float32Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::table::{SchemaManager, Table};

const VECTOR_COLUMN: &str = "embedding";
const FIXTURE: &str = "testdata/pkvector/pk_vector_ivf_flat";

/// The exact vectors the Java generator wrote, `id == row position`. Query is
/// `[0, 0]`; squared-L2 distances are the squared norms `[0, 1, 4, 9, 16]`.
const VECTORS: &[[f32; 2]] = &[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0], [4.0, 0.0]];

fn l2_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}

/// Brute-force exact squared-L2 top-k over the fixture rows -> `(position, distance)`
/// ascending by distance. `position == vector index == id` because the fixture
/// pins `id == row position`.
fn analytic_topk(query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = VECTORS
        .iter()
        .enumerate()
        .map(|(pos, v)| {
            let dist: f32 = v
                .iter()
                .zip(query.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum();
            (pos as u64, dist)
        })
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1));
    scored.truncate(k);
    scored
}

/// Open the committed Java-written table directory. Copies the fixture into a
/// fresh temp dir first so the read path has a private `file://` root (mirrors
/// how `global_index_scanner.rs` fixtures stage into a temp dir), and so a stray
/// write during read can never mutate the committed testdata.
async fn open_java_fixture() -> (tempfile::TempDir, Table) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src = Path::new(manifest_dir).join(FIXTURE);
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dst = tmp.path().join("pk_vector_ivf_flat");
    copy_dir(&src, &dst);

    let location = format!("file://{}", dst.display());
    let file_io: FileIO = FileIOBuilder::new("file").build().expect("build fs FileIO");
    let schema = SchemaManager::new(file_io.clone(), location.clone())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("fixture table has no schema");
    let table = Table::new(
        file_io,
        Identifier::new("default", "pk_vector_ivf_flat"),
        location,
        (*schema).clone(),
        None,
    );
    (tmp, table)
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

fn batch_i32(batches: &[RecordBatch], col: &str) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn batch_f32(batches: &[RecordBatch], col: &str) -> Vec<f32> {
    batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

// Gated off Windows: the fixture is opened via a `file://` URL built from a
// tempdir path, matching how the sibling `pk_vector_baseline_test` and
// `rest_catalog_test` gate their `file://` tempdir tests.
#[cfg(not(target_os = "windows"))]
#[tokio::test]
async fn reads_back_java_written_pk_vector_table() {
    let (_tmp, table) = open_java_fixture().await;
    let query = vec![0.0f32, 0.0];
    let k = 3;

    let expected = analytic_topk(&query, k);
    let expected_ids: Vec<i32> = expected.iter().map(|(id, _)| *id as i32).collect();
    let expected_scores: Vec<f32> = expected.iter().map(|(_, d)| l2_score(*d)).collect();
    // Guard against silent drift of the analytic ground truth.
    assert_eq!(
        expected_ids,
        vec![0, 1, 2],
        "fixture top-3 ids must be [0, 1, 2]"
    );

    // Java PK files need no first_row_id: the result retains physical positions.
    let scored = table
        .new_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query.clone())
        .with_limit(k)
        .execute()
        .await
        .unwrap();
    assert!(scored.row_ids().is_err());
    assert_eq!(
        scored
            .positions()
            .unwrap()
            .iter()
            .map(|p| p.row_position as i32)
            .collect::<Vec<_>>(),
        expected_ids
    );

    // the result reader materializes rows by physical position, so it MUST succeed
    // and emit the top-k best-first. The `id` column cross-checks the
    // position->id mapping (the fixture pins them equal) and `__paimon_search_score`
    // carries the metric score.
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(k);
    let batches = async {
        builder
            .execute()
            .await?
            .new_read_builder()
            .with_projection(&["id"])
            .read()
            .await
    }
    .await
    .expect("primary-key vector read over the Java fixture failed")
    .try_collect::<Vec<_>>()
    .await
    .expect("collecting read batches failed");

    let ids = batch_i32(&batches, "id");
    assert_eq!(
        ids, expected_ids,
        "materialized `id` column must be best-first and match the analytic top-k"
    );

    let scores = batch_f32(&batches, "__paimon_search_score");
    assert_eq!(scores.len(), k);
    for (got, want) in scores.iter().zip(&expected_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "materialized score diverges: got {got}, want {want}"
        );
    }
}
