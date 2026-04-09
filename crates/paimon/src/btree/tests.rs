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

use crate::btree::block::BlockCompressionType;
use crate::btree::meta::BTreeIndexMeta;
use crate::btree::reader::BTreeIndexReader;
use crate::btree::test_util::{BytesFileRead, VecFileWrite};
use crate::btree::writer::BTreeIndexWriter;
use bytes::Bytes;

fn int_key(v: i32) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn int_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a_val = i32::from_be_bytes(a.try_into().unwrap());
    let b_val = i32::from_be_bytes(b.try_into().unwrap());
    a_val.cmp(&b_val)
}

/// Helper: write entries, finish, then open a reader from the in-memory bytes.
async fn write_and_open<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
    buf: &VecFileWrite,
    result: &crate::btree::writer::BTreeWriteResult,
    cmp: F,
) -> BTreeIndexReader<F> {
    let data = Bytes::from(buf.to_vec());
    let file_size = data.len() as u64;
    let reader: Box<dyn crate::io::FileRead> = Box::new(BytesFileRead(data));
    BTreeIndexReader::open(reader, file_size, &result.meta, cmp)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_write_read_roundtrip() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    for i in 0..100 {
        let key = int_key(i);
        writer.write(Some(&key), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    assert_eq!(result.row_count, 100);
    assert!(!result.meta.has_nulls);

    let reader = write_and_open(&buf, &result, int_cmp).await;

    // Spot check via queries
    let bm = reader.query_equal(&int_key(0)).await.unwrap();
    assert!(bm.contains(0));
    let bm = reader.query_equal(&int_key(99)).await.unwrap();
    assert!(bm.contains(99));
    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 100);
}

#[tokio::test]
async fn test_duplicate_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    let key = int_key(42);
    writer.write(Some(&key), 100).await.unwrap();
    writer.write(Some(&key), 200).await.unwrap();
    writer.write(Some(&key), 300).await.unwrap();

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(42)).await.unwrap();
    assert_eq!(bm.len(), 3);
    assert!(bm.contains(100));
    assert!(bm.contains(200));
    assert!(bm.contains(300));
}

#[tokio::test]
async fn test_null_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    writer.write(None, 10).await.unwrap();
    writer.write(Some(&int_key(1)), 20).await.unwrap();
    writer.write(None, 30).await.unwrap();
    writer.write(Some(&int_key(2)), 40).await.unwrap();

    let result = writer.finish().await.unwrap();
    assert_eq!(result.row_count, 4);
    assert!(result.meta.has_nulls);

    let reader = write_and_open(&buf, &result, int_cmp).await;

    let null_bm = reader.null_bitmap();
    assert!(null_bm.contains(10));
    assert!(null_bm.contains(30));
    assert!(!null_bm.contains(20));
    assert!(!null_bm.contains(40));

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn test_only_nulls() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    writer.write(None, 1).await.unwrap();
    writer.write(None, 2).await.unwrap();
    writer.write(None, 3).await.unwrap();

    let result = writer.finish().await.unwrap();
    assert!(result.meta.only_nulls());
    assert!(result.meta.has_nulls);

    let reader = write_and_open(&buf, &result, int_cmp).await;
    let null_bm = reader.null_bitmap();
    assert_eq!(null_bm.len(), 3);
}

#[tokio::test]
async fn test_equal_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    for i in 0..50 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(25)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(25));

    let bm = reader.query_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_range_queries() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    for i in 0..20 {
        writer
            .write(Some(&int_key(i * 10)), i as i64)
            .await
            .unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_less_than(&int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 5);

    let bm = reader.query_greater_or_equal(&int_key(150)).await.unwrap();
    assert_eq!(bm.len(), 5);

    let bm = reader
        .query_between(&int_key(30), &int_key(70))
        .await
        .unwrap();
    assert_eq!(bm.len(), 5);
}

#[tokio::test]
async fn test_not_equal_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    for i in 0..5 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_not_equal(&int_key(2)).await.unwrap();
    assert_eq!(bm.len(), 4);
    assert!(!bm.contains(2));
}

#[tokio::test]
async fn test_in_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    for i in 0..10 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let keys: Vec<Vec<u8>> = vec![int_key(2), int_key(5), int_key(8)];
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let bm = reader.query_in(&key_refs).await.unwrap();
    assert_eq!(bm.len(), 3);
    assert!(bm.contains(2));
    assert!(bm.contains(5));
    assert!(bm.contains(8));
}

#[tokio::test]
async fn test_string_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    let keys = ["apple", "banana", "cherry", "date", "elderberry"];
    for (i, k) in keys.iter().enumerate() {
        writer.write(Some(k.as_bytes()), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, |a, b| a.cmp(b)).await;

    let bm = reader.query_equal(b"apple").await.unwrap();
    assert!(bm.contains(0));
    let bm = reader.query_equal(b"elderberry").await.unwrap();
    assert!(bm.contains(4));
}

#[tokio::test]
async fn test_string_keys_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);

    let keys = ["apple", "banana", "cherry", "date", "elderberry"];
    for (i, k) in keys.iter().enumerate() {
        writer.write(Some(k.as_bytes()), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, |a, b| a.cmp(b)).await;

    let bm = reader.query_equal(b"cherry").await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(2));
}

#[tokio::test]
async fn test_large_dataset() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 4096, BlockCompressionType::None);

    let n = 10000;
    for i in 0..n {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    assert_eq!(result.row_count, n as u64);

    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(5000)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(5000));

    let bm = reader
        .query_between(&int_key(100), &int_key(199))
        .await
        .unwrap();
    assert_eq!(bm.len(), 100);
}

#[test]
fn test_meta_serialization() {
    let meta = BTreeIndexMeta::new(Some(int_key(0)), Some(int_key(99)), false);
    let bytes = meta.serialize();
    let decoded = BTreeIndexMeta::deserialize(&bytes).unwrap();
    assert_eq!(decoded.first_key, Some(int_key(0)));
    assert_eq!(decoded.last_key, Some(int_key(99)));
    assert!(!decoded.has_nulls);
}

#[tokio::test]
async fn test_zstd_compression() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::Zstd);

    for i in 0..100 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 100);

    let bm = reader.query_equal(&int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(50));
}

#[tokio::test]
async fn test_single_entry() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);
    writer.write(Some(&int_key(42)), 99).await.unwrap();

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(42)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(99));

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn test_boundary_queries() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);
    for i in 10..20 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_less_than(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 0);

    let bm = reader.query_greater_than(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 0);

    let bm = reader.query_equal(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 1);

    let bm = reader.query_equal(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 1);

    let bm = reader.query_less_or_equal(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 1);

    let bm = reader.query_greater_or_equal(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 1);
}

#[tokio::test]
async fn test_large_row_ids() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    let large_ids: Vec<i64> = vec![0, i32::MAX as i64, i64::MAX / 2];
    for (i, &id) in large_ids.iter().enumerate() {
        writer.write(Some(&int_key(i as i32)), id).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(0)).await.unwrap();
    assert!(bm.contains(0));
    let bm = reader.query_equal(&int_key(1)).await.unwrap();
    assert!(bm.contains(i32::MAX as u64));
    let bm = reader.query_equal(&int_key(2)).await.unwrap();
    assert!(bm.contains((i64::MAX / 2) as u64));
}

#[tokio::test]
async fn test_many_duplicate_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    for i in 0..100 {
        writer.write(Some(&int_key(1)), i).await.unwrap();
    }

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(1)).await.unwrap();
    assert_eq!(bm.len(), 100);
}

#[tokio::test]
async fn test_nulls_with_range_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 256, BlockCompressionType::None);

    writer.write(None, 0).await.unwrap();
    for i in 1..=5 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }
    writer.write(None, 100).await.unwrap();

    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 5);
    assert!(!all.contains(0));
    assert!(!all.contains(100));

    let nulls = reader.null_bitmap();
    assert_eq!(nulls.len(), 2);
    assert!(nulls.contains(0));
    assert!(nulls.contains(100));
}

#[tokio::test]
async fn test_seek_before_all_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);
    for i in 100..110 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_greater_or_equal(&int_key(0)).await.unwrap();
    assert_eq!(bm.len(), 10);
}

#[tokio::test]
async fn test_seek_after_all_keys() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 64, BlockCompressionType::None);
    for i in 0..10 {
        writer.write(Some(&int_key(i)), i as i64).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader.query_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);

    let bm = reader.query_greater_or_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_multiple_blocks_range_query() {
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::new(Box::new(buf.clone()), 16, BlockCompressionType::None);
    for i in 0..50 {
        writer.write(Some(&int_key(i * 2)), i as i64).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let reader = write_and_open(&buf, &result, int_cmp).await;

    let bm = reader
        .query_between(&int_key(20), &int_key(40))
        .await
        .unwrap();
    assert_eq!(bm.len(), 11);

    let bm = reader.query_equal(&int_key(3)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

// ============================================================
// Java compatibility tests — read files generated by Java Paimon
// ============================================================

fn le_int_key(v: i32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn le_int_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a_val = i32::from_le_bytes(a.try_into().unwrap());
    let b_val = i32::from_le_bytes(b.try_into().unwrap());
    a_val.cmp(&b_val)
}

fn load_testdata(name: &str) -> Vec<u8> {
    let path = format!("{}/testdata/btree/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
}

async fn open_testdata<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
    name: &str,
    meta: &BTreeIndexMeta,
    cmp: F,
) -> BTreeIndexReader<F> {
    let data = Bytes::from(load_testdata(name));
    let file_size = data.len() as u64;
    let reader: Box<dyn crate::io::FileRead> = Box::new(BytesFileRead(data));
    BTreeIndexReader::open(reader, file_size, meta, cmp)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_java_compat_int_no_compress() {
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let reader = open_testdata("btree_int_100_no_compress.bin", &meta, le_int_cmp).await;

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 100);

    let bm = reader.query_equal(&le_int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(25));

    let bm = reader
        .query_between(&le_int_key(10), &le_int_key(20))
        .await
        .unwrap();
    assert_eq!(bm.len(), 6);

    let bm = reader.query_equal(&le_int_key(1)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_java_compat_int_zstd() {
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let reader = open_testdata("btree_int_100_zstd.bin", &meta, le_int_cmp).await;

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 100);

    let bm = reader.query_equal(&le_int_key(100)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(50));
}

#[tokio::test]
async fn test_java_compat_int_with_nulls() {
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), true);
    let reader = open_testdata("btree_int_100_with_nulls.bin", &meta, le_int_cmp).await;

    let null_bm = reader.null_bitmap();
    assert!(!null_bm.is_empty());

    let all = reader.all_non_null_rows().await.unwrap();
    assert!(!all.is_empty());

    let total = null_bm.len() + all.len();
    assert!(total > 0);

    let bm = reader.query_equal(&le_int_key(0)).await.unwrap();
    assert_eq!(bm.len(), 1);

    for null_id in null_bm.iter() {
        assert!(!all.contains(null_id));
    }
}

#[tokio::test]
async fn test_java_compat_varchar_no_compress() {
    let meta = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"yyyy".to_vec()), false);
    let reader = open_testdata("btree_varchar_100_no_compress.bin", &meta, |a, b| a.cmp(b)).await;

    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 100);

    let bm = reader.query_equal(b"a").await.unwrap();
    assert_eq!(bm.len(), 1);
}

#[tokio::test]
async fn test_java_compat_int_lz4_unsupported() {
    let data = Bytes::from(load_testdata("btree_int_100_lz4.bin"));
    let file_size = data.len() as u64;
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let reader_result =
        BTreeIndexReader::open(Box::new(BytesFileRead(data)), file_size, &meta, le_int_cmp).await;

    match reader_result {
        Err(e) => {
            assert!(
                e.to_string().contains("not supported") || e.to_string().contains("Unsupported"),
                "Expected unsupported compression error, got: {e}"
            );
        }
        Ok(reader) => {
            // If reader creation succeeded (index block not compressed),
            // data block read should fail
            let result = reader.query_equal(&le_int_key(0)).await;
            assert!(result.is_err(), "LZ4 data block read should fail");
        }
    }
}
