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

//! Bidirectional Java ↔ Rust compatibility tests for sketch/bitmap aggregators.
//!
//! - **Java → Rust**: production Rust aggregators consume Java golden bytes.
//! - **Rust → Java**: Rust writes payloads to a temp directory, then
//!   `VerifyRustSketchBitmapCompat.java` deserializes them with the same
//!   production library call patterns as Paimon `HllSketchUtil` / `ThetaSketch`
//!   / `RoaringBitmap32` / `RoaringBitmap64` (DataSketches 4.2.0, Roaring 1.2.1).
//!
//! JVM classpath comes from `goldens/pom.xml` via Maven (`dependency:build-classpath`),
//! matching `paimon-common` versions. Do not commit jars under `goldens/lib/`.
//!
//! Fixtures: `GenerateSketchBitmapCompat.java` (deterministic, non-random).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Array, BinaryArray};
use roaring::RoaringBitmap;

use super::bitmap::Roaring32Agg;
use super::bitmap64::{self, Roaring64Agg};
use super::sketch::{self, HllSketchAgg, ThetaSketchAgg};
use super::FieldAggregator;
use crate::spec::{DataType, VarBinaryType};

fn varbinary() -> DataType {
    DataType::VarBinary(VarBinaryType::new(65535).unwrap())
}

fn goldens_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/table/goldens")
}

fn rust_written_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("paimon-rust-sketch-bitmap-compat-{nanos}-{n}"))
}

fn merge_rows(agg: &mut dyn FieldAggregator, rows: &[&[u8]]) {
    let input = BinaryArray::from(rows.iter().copied().map(Some).collect::<Vec<_>>());
    for idx in 0..rows.len() {
        agg.agg(&input, idx).unwrap();
    }
}

fn result_bytes(agg: &dyn FieldAggregator) -> Vec<u8> {
    let result = agg.result().unwrap();
    let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert!(!result.is_null(0));
    result.value(0).to_vec()
}

fn rbm32_values(bytes: &[u8]) -> BTreeSet<u32> {
    RoaringBitmap::deserialize_from(bytes)
        .unwrap()
        .iter()
        .collect()
}

fn serialize_rbm32(values: &[u32]) -> Vec<u8> {
    let mut bitmap = RoaringBitmap::new();
    for value in values {
        bitmap.insert(*value);
    }
    bitmap.optimize();
    let mut bytes = Vec::new();
    bitmap.serialize_into(&mut bytes).unwrap();
    bytes
}

fn write_bytes(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, bytes).unwrap();
}

/// Emit Rust-written payloads for the JVM verifier (deterministic inputs).
fn emit_rust_written_fixtures() -> PathBuf {
    let out = rust_written_dir();
    std::fs::create_dir_all(&out).unwrap();

    let rust_a = serialize_rbm32(&[1, 2, 3, 100]);
    let rust_b = serialize_rbm32(&[3, 4, 5, 100]);
    write_bytes(&out.join("rust_rbm32_a.bin"), &rust_a);
    write_bytes(&out.join("rust_rbm32_b.bin"), &rust_b);
    let mut rbm32 = Roaring32Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut rbm32, &[&rust_a, &rust_b]);
    write_bytes(&out.join("rust_rbm32_union.bin"), &result_bytes(&rbm32));

    let values_a = [1u64, 2, 3, (1 << 32) + 5];
    let values_b = [3u64, 4, (1 << 32) + 5, 1 << 40];
    let rust64_a = bitmap64::test_support::encode_values(values_a).unwrap();
    let rust64_b = bitmap64::test_support::encode_values(values_b).unwrap();
    write_bytes(&out.join("rust_rbm64_a.bin"), &rust64_a);
    write_bytes(&out.join("rust_rbm64_b.bin"), &rust64_b);
    let mut rbm64 = Roaring64Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut rbm64, &[&rust64_a, &rust64_b]);
    write_bytes(&out.join("rust_rbm64_union.bin"), &result_bytes(&rbm64));

    let hll_a = include_bytes!("../goldens/compat_hll_a.bin");
    let hll_b = include_bytes!("../goldens/compat_hll_b.bin");
    let mut hll = HllSketchAgg::new("s", &varbinary()).unwrap();
    merge_rows(&mut hll, &[hll_a, hll_b]);
    write_bytes(&out.join("rust_hll_union.bin"), &result_bytes(&hll));

    let theta_a = include_bytes!("../goldens/compat_theta_a.bin");
    let theta_b = include_bytes!("../goldens/compat_theta_b.bin");
    let mut theta = ThetaSketchAgg::new("s", &varbinary()).unwrap();
    merge_rows(&mut theta, &[theta_a, theta_b]);
    write_bytes(&out.join("rust_theta_union.bin"), &result_bytes(&theta));

    out
}

fn maven_command() -> Command {
    // On Windows, `Command::new("mvn")` does not automatically resolve `mvn.cmd`.
    let candidates = if cfg!(windows) {
        ["mvn.cmd", "mvn"].as_slice()
    } else {
        ["mvn"].as_slice()
    };
    for name in candidates {
        if Command::new(name)
            .arg("-v")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return Command::new(name);
        }
    }
    panic!(
        "mvn not found on PATH (tried {:?}); install Maven to resolve goldens/pom.xml deps",
        candidates
    );
}

/// Resolve RoaringBitmap 1.2.1 + datasketches-java 4.2.0 via `goldens/pom.xml`.
fn maven_classpath() -> String {
    let goldens = goldens_dir();
    let cp_file =
        std::env::temp_dir().join(format!("paimon-rust-compat-cp-{}.txt", std::process::id()));
    let output = maven_command()
        .current_dir(&goldens)
        .args([
            "-q",
            "dependency:build-classpath",
            &format!("-Dmdep.outputFile={}", cp_file.display()),
        ])
        .output()
        .expect("failed to execute mvn dependency:build-classpath");
    assert!(
        output.status.success(),
        "mvn dependency:build-classpath failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let cp = std::fs::read_to_string(&cp_file).unwrap_or_else(|e| {
        panic!(
            "failed to read Maven classpath file {}: {e}",
            cp_file.display()
        )
    });
    let _ = std::fs::remove_file(&cp_file);
    let cp = cp.trim().to_string();
    assert!(
        !cp.is_empty(),
        "Maven classpath for goldens/pom.xml was empty"
    );
    cp
}

/// Compile and run `VerifyRustSketchBitmapCompat.java` against goldens + rust temp dir.
fn run_jvm_verifier(rust_out: &Path) {
    let goldens = goldens_dir();
    let cp = maven_classpath();

    let verify_src = goldens.join("VerifyRustSketchBitmapCompat.java");
    let status = Command::new("javac")
        .current_dir(&goldens)
        .args([
            "-cp",
            &cp,
            verify_src.file_name().unwrap().to_str().unwrap(),
        ])
        .status()
        .expect("javac must be available for JVM compatibility verification");
    assert!(
        status.success(),
        "javac VerifyRustSketchBitmapCompat failed"
    );

    let sep = if cfg!(windows) { ';' } else { ':' };
    let run_cp = format!(".{sep}{cp}");
    let output = Command::new("java")
        .current_dir(&goldens)
        .args([
            "-cp",
            &run_cp,
            "VerifyRustSketchBitmapCompat",
            ".",
            rust_out.to_str().unwrap(),
        ])
        .output()
        .expect("java must be available for JVM compatibility verification");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "JVM verifier failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("OK all JVM compatibility checks passed"),
        "unexpected JVM verifier output:\n{stdout}\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// Java → Rust (production Rust aggregators)
// ---------------------------------------------------------------------------

#[test]
fn rbm32_java_to_rust_merge_matches_java_union_set() {
    let a = include_bytes!("../goldens/compat_rbm32_a.bin");
    let b = include_bytes!("../goldens/compat_rbm32_b.bin");
    let expected = include_bytes!("../goldens/compat_rbm32_union.bin");
    let mut agg = Roaring32Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut agg, &[a, b]);
    assert_eq!(rbm32_values(&result_bytes(&agg)), rbm32_values(expected));
}

#[test]
fn rbm64_java_to_rust_merge_matches_java_union_set() {
    let a = include_bytes!("../goldens/compat_rbm64_a.bin");
    let b = include_bytes!("../goldens/compat_rbm64_b.bin");
    let expected = include_bytes!("../goldens/compat_rbm64_union.bin");
    let mut agg = Roaring64Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut agg, &[a, b]);
    assert_eq!(
        bitmap64::test_support::decode_to_values(&result_bytes(&agg)).unwrap(),
        bitmap64::test_support::decode_to_values(expected).unwrap()
    );
}

#[test]
fn hll_java_to_rust_merge_matches_java_union_estimate() {
    let a = include_bytes!("../goldens/compat_hll_a.bin");
    let b = include_bytes!("../goldens/compat_hll_b.bin");
    let java_union = include_bytes!("../goldens/compat_hll_union.bin");
    let expected = sketch::test_support::hll_estimate(java_union);

    let mut agg = HllSketchAgg::new("s", &varbinary()).unwrap();
    merge_rows(&mut agg, &[a, b]);
    let actual = sketch::test_support::hll_estimate(&result_bytes(&agg));
    assert!(
        (actual - expected).abs() < 1e-9,
        "rust={actual} java={expected}"
    );
}

#[test]
fn theta_java_to_rust_merge_matches_java_union_entries() {
    let a = include_bytes!("../goldens/compat_theta_a.bin");
    let b = include_bytes!("../goldens/compat_theta_b.bin");
    let expected =
        sketch::test_support::theta_entries(include_bytes!("../goldens/compat_theta_union.bin"));

    let mut agg = ThetaSketchAgg::new("s", &varbinary()).unwrap();
    merge_rows(&mut agg, &[a, b]);
    let actual = sketch::test_support::theta_entries(&result_bytes(&agg));
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 15);
}

// ---------------------------------------------------------------------------
// Edge cases on Rust production path (Java fixtures)
// ---------------------------------------------------------------------------

#[test]
fn rbm32_empty_duplicates_bounds_and_multi_merge() {
    let empty = include_bytes!("../goldens/compat_rbm32_empty.bin");
    let dups = include_bytes!("../goldens/compat_rbm32_dups.bin");
    let bounds = include_bytes!("../goldens/compat_rbm32_bounds.bin");
    let a = include_bytes!("../goldens/compat_rbm32_a.bin");
    let b = include_bytes!("../goldens/compat_rbm32_b.bin");

    assert!(rbm32_values(empty).is_empty());
    assert_eq!(rbm32_values(dups), BTreeSet::from([7, 8]));
    assert_eq!(
        rbm32_values(bounds),
        BTreeSet::from([0, i32::MAX as u32, u32::MAX])
    );

    let mut agg = Roaring32Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut agg, &[empty, dups, a, b, bounds, dups]);
    assert_eq!(
        rbm32_values(&result_bytes(&agg)),
        BTreeSet::from([0, 1, 2, 3, 4, 5, 7, 8, 100, i32::MAX as u32, u32::MAX])
    );
}

#[test]
fn rbm64_empty_and_multi_merge_cardinality() {
    let empty = include_bytes!("../goldens/compat_rbm64_empty.bin");
    let a = include_bytes!("../goldens/compat_rbm64_a.bin");
    let b = include_bytes!("../goldens/compat_rbm64_b.bin");
    assert_eq!(empty, &[0]);
    assert!(bitmap64::test_support::decode_to_values(empty)
        .unwrap()
        .is_empty());

    let mut agg = Roaring64Agg::new("bm", &varbinary()).unwrap();
    merge_rows(&mut agg, &[empty, a, empty, b, a]);
    assert_eq!(
        bitmap64::test_support::decode_to_values(&result_bytes(&agg))
            .unwrap()
            .len(),
        6
    );
}

#[test]
fn hll_empty_null_and_multi_merge() {
    let empty = include_bytes!("../goldens/compat_hll_empty.bin");
    let a = include_bytes!("../goldens/compat_hll_a.bin");
    let b = include_bytes!("../goldens/compat_hll_b.bin");
    let null_row = BinaryArray::from(vec![None::<&[u8]>, Some(a.as_slice()), Some(b.as_slice())]);

    let mut agg = HllSketchAgg::new("s", &varbinary()).unwrap();
    agg.agg(&null_row, 0).unwrap();
    assert!(agg.result().unwrap().is_null(0));

    agg.reset();
    merge_rows(&mut agg, &[empty, a, empty, b]);
    let estimate = sketch::test_support::hll_estimate(&result_bytes(&agg));
    let java_union =
        sketch::test_support::hll_estimate(include_bytes!("../goldens/compat_hll_union.bin"));
    assert!(
        (estimate - java_union).abs() < 1e-6,
        "{estimate} vs {java_union}"
    );
}

#[test]
fn theta_empty_and_multi_merge() {
    let empty = include_bytes!("../goldens/compat_theta_empty.bin");
    let a = include_bytes!("../goldens/compat_theta_a.bin");
    let b = include_bytes!("../goldens/compat_theta_b.bin");
    assert!(sketch::test_support::theta_emptyish(empty));

    let mut agg = ThetaSketchAgg::new("s", &varbinary()).unwrap();
    merge_rows(&mut agg, &[empty, a, empty, b, a]);
    assert_eq!(
        sketch::test_support::theta_entries(&result_bytes(&agg)).len(),
        15
    );
}

// ---------------------------------------------------------------------------
// Rust → Java via real JVM (production DataSketches / Roaring deserializers)
// ---------------------------------------------------------------------------

#[test]
fn rust_written_bytes_pass_jvm_production_reader_and_merge() {
    let rust_out = emit_rust_written_fixtures();
    run_jvm_verifier(&rust_out);
}
