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
use crate::btree::writer::BTreeIndexWriter;

fn int_key(v: i32) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn int_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a_val = i32::from_be_bytes(a.try_into().unwrap());
    let b_val = i32::from_be_bytes(b.try_into().unwrap());
    a_val.cmp(&b_val)
}

#[test]
fn test_write_read_roundtrip() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    for i in 0..100 {
        let key = int_key(i);
        writer.write(Some(&key), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    assert_eq!(result.row_count, 100);
    assert!(!result.meta.has_nulls);

    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Iterate all entries
    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 100);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.key, int_key(i as i32));
        assert_eq!(entry.row_ids, vec![i as i64]);
    }
}

#[test]
fn test_duplicate_keys() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    // Write same key with multiple row ids
    let key = int_key(42);
    writer.write(Some(&key), 100).unwrap();
    writer.write(Some(&key), 200).unwrap();
    writer.write(Some(&key), 300).unwrap();

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, int_key(42));
    assert_eq!(entries[0].row_ids, vec![100, 200, 300]);
}

#[test]
fn test_null_keys() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    writer.write(None, 10).unwrap();
    writer.write(Some(&int_key(1)), 20).unwrap();
    writer.write(None, 30).unwrap();
    writer.write(Some(&int_key(2)), 40).unwrap();

    let result = writer.finish().unwrap();
    assert_eq!(result.row_count, 4);
    assert!(result.meta.has_nulls);

    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Check null bitmap
    let null_bm = reader.null_bitmap();
    assert!(null_bm.contains(10));
    assert!(null_bm.contains(30));
    assert!(!null_bm.contains(20));
    assert!(!null_bm.contains(40));

    // Check non-null entries
    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 2);
}

#[test]
fn test_only_nulls() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    writer.write(None, 1).unwrap();
    writer.write(None, 2).unwrap();
    writer.write(None, 3).unwrap();

    let result = writer.finish().unwrap();
    assert!(result.meta.only_nulls());
    assert!(result.meta.has_nulls);

    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let null_bm = reader.null_bitmap();
    assert_eq!(null_bm.len(), 3);
}

#[tokio::test]
async fn test_equal_query() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    for i in 0..50 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let bm = reader.query_equal(&int_key(25)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(25));

    // Non-existent key
    let bm = reader.query_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_range_queries() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    for i in 0..20 {
        writer.write(Some(&int_key(i * 10)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Less than 50 -> keys 0,10,20,30,40 -> row ids 0,1,2,3,4
    let bm = reader.query_less_than(&int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 5);
    for i in 0..5u64 {
        assert!(bm.contains(i));
    }

    // Greater or equal 150 -> keys 150,160,170,180,190 -> row ids 15,16,17,18,19
    let bm = reader.query_greater_or_equal(&int_key(150)).await.unwrap();
    assert_eq!(bm.len(), 5);
    for i in 15..20u64 {
        assert!(bm.contains(i));
    }

    // Between 30 and 70 -> keys 30,40,50,60,70 -> row ids 3,4,5,6,7
    let bm = reader
        .query_between(&int_key(30), &int_key(70))
        .await
        .unwrap();
    assert_eq!(bm.len(), 5);
    for i in 3..8u64 {
        assert!(bm.contains(i));
    }
}

#[tokio::test]
async fn test_not_equal_query() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    for i in 0..5 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let bm = reader.query_not_equal(&int_key(2)).await.unwrap();
    assert_eq!(bm.len(), 4);
    assert!(bm.contains(0));
    assert!(bm.contains(1));
    assert!(!bm.contains(2));
    assert!(bm.contains(3));
    assert!(bm.contains(4));
}

#[tokio::test]
async fn test_in_query() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    for i in 0..10 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let keys: Vec<Vec<u8>> = vec![int_key(2), int_key(5), int_key(8)];
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let bm = reader.query_in(&key_refs).await.unwrap();
    assert_eq!(bm.len(), 3);
    assert!(bm.contains(2));
    assert!(bm.contains(5));
    assert!(bm.contains(8));
}

#[test]
fn test_string_keys() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    let keys = ["apple", "banana", "cherry", "date", "elderberry"];
    for (i, k) in keys.iter().enumerate() {
        writer.write(Some(k.as_bytes()), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, |a, b| a.cmp(b)).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].key, b"apple");
    assert_eq!(entries[4].key, b"elderberry");
}

#[tokio::test]
async fn test_string_keys_query() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);

    let keys = ["apple", "banana", "cherry", "date", "elderberry"];
    for (i, k) in keys.iter().enumerate() {
        writer.write(Some(k.as_bytes()), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, |a, b| a.cmp(b)).unwrap();

    let bm = reader.query_equal(b"cherry").await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(2));
}

#[tokio::test]
async fn test_large_dataset() {
    let mut writer = BTreeIndexWriter::new(4096, BlockCompressionType::None);

    let n = 10000;
    for i in 0..n {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    assert_eq!(result.row_count, n as u64);

    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Spot check
    let bm = reader.query_equal(&int_key(5000)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(5000));

    // Range
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
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::Zstd);

    for i in 0..100 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 100);

    let bm = reader.query_equal(&int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(50));
}

#[tokio::test]
async fn test_single_entry() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);
    writer.write(Some(&int_key(42)), 99).unwrap();

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let bm = reader.query_equal(&int_key(42)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(99));

    // All non-null rows
    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn test_boundary_queries() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);
    for i in 10..20 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }
    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Less than first key -> empty
    let bm = reader.query_less_than(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 0);

    // Greater than last key -> empty
    let bm = reader.query_greater_than(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 0);

    // Equal to first key
    let bm = reader.query_equal(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(10));

    // Equal to last key
    let bm = reader.query_equal(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(19));

    // Less or equal to first key -> just first
    let bm = reader.query_less_or_equal(&int_key(10)).await.unwrap();
    assert_eq!(bm.len(), 1);

    // Greater or equal to last key -> just last
    let bm = reader.query_greater_or_equal(&int_key(19)).await.unwrap();
    assert_eq!(bm.len(), 1);
}

#[test]
fn test_large_row_ids() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    let large_ids: Vec<i64> = vec![0, i32::MAX as i64, i64::MAX / 2];
    for (i, &id) in large_ids.iter().enumerate() {
        writer.write(Some(&int_key(i as i32)), id).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].row_ids, vec![0]);
    assert_eq!(entries[1].row_ids, vec![i32::MAX as i64]);
    assert_eq!(entries[2].row_ids, vec![i64::MAX / 2]);
}

#[tokio::test]
async fn test_many_duplicate_keys() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    // 100 row ids for the same key
    for i in 0..100 {
        writer.write(Some(&int_key(1)), i).unwrap();
    }

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].row_ids.len(), 100);

    let bm = reader.query_equal(&int_key(1)).await.unwrap();
    assert_eq!(bm.len(), 100);
}

#[tokio::test]
async fn test_nulls_with_range_query() {
    let mut writer = BTreeIndexWriter::new(256, BlockCompressionType::None);

    writer.write(None, 0).unwrap();
    for i in 1..=5 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }
    writer.write(None, 100).unwrap();

    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Range query should NOT include nulls
    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), 5);
    assert!(!all.contains(0));
    assert!(!all.contains(100));

    // Null bitmap should have the null row ids
    let nulls = reader.null_bitmap();
    assert_eq!(nulls.len(), 2);
    assert!(nulls.contains(0));
    assert!(nulls.contains(100));
}

#[tokio::test]
async fn test_seek_before_all_keys() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);
    for i in 100..110 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }
    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Query with key smaller than all -> should get all via greater_or_equal
    let bm = reader.query_greater_or_equal(&int_key(0)).await.unwrap();
    assert_eq!(bm.len(), 10);
}

#[tokio::test]
async fn test_seek_after_all_keys() {
    let mut writer = BTreeIndexWriter::new(64, BlockCompressionType::None);
    for i in 0..10 {
        writer.write(Some(&int_key(i)), i as i64).unwrap();
    }
    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Query with key larger than all -> empty
    let bm = reader.query_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);

    let bm = reader.query_greater_or_equal(&int_key(999)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_multiple_blocks_range_query() {
    // Use very small block size to force many blocks
    let mut writer = BTreeIndexWriter::new(16, BlockCompressionType::None);
    for i in 0..50 {
        writer.write(Some(&int_key(i * 2)), i as i64).unwrap();
    }
    let result = writer.finish().unwrap();
    let reader = BTreeIndexReader::new(result.file_data, &result.meta, int_cmp).unwrap();

    // Range spanning multiple blocks: keys 20..=40 -> row ids 10..=20
    let bm = reader
        .query_between(&int_key(20), &int_key(40))
        .await
        .unwrap();
    assert_eq!(bm.len(), 11);
    for i in 10..=20u64 {
        assert!(bm.contains(i), "missing row id {i}");
    }

    // Query for non-existent key between existing keys
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

#[tokio::test]
async fn test_java_compat_int_no_compress() {
    let data = load_testdata("btree_int_100_no_compress.bin");
    // 100 keys: key_i = i*2 (LE i32), row_id_i = i
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let reader = BTreeIndexReader::new(data, &meta, le_int_cmp).unwrap();

    // Verify all entries
    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 100);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.key, le_int_key((i * 2) as i32));
        assert_eq!(entry.row_ids, vec![i as i64]);
    }

    // Point query
    let bm = reader.query_equal(&le_int_key(50)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(25));

    // Range query
    let bm = reader
        .query_between(&le_int_key(10), &le_int_key(20))
        .await
        .unwrap();
    assert_eq!(bm.len(), 6); // keys 10,12,14,16,18,20 -> row_ids 5,6,7,8,9,10

    // Non-existent key
    let bm = reader.query_equal(&le_int_key(1)).await.unwrap();
    assert_eq!(bm.len(), 0);
}

#[tokio::test]
async fn test_java_compat_int_zstd() {
    let data = load_testdata("btree_int_100_zstd.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    let reader = BTreeIndexReader::new(data, &meta, le_int_cmp).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 100);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.key, le_int_key((i * 2) as i32));
        assert_eq!(entry.row_ids, vec![i as i64]);
    }

    let bm = reader.query_equal(&le_int_key(100)).await.unwrap();
    assert_eq!(bm.len(), 1);
    assert!(bm.contains(50));
}

#[tokio::test]
async fn test_java_compat_int_with_nulls() {
    let data = load_testdata("btree_int_100_with_nulls.bin");
    // Java generator: 100 non-null keys (key_i = i*2) + 20 null entries = 120 total rows
    // Non-null keys range: 0..198
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), true);
    let reader = BTreeIndexReader::new(data, &meta, le_int_cmp).unwrap();

    // Should have nulls
    let null_bm = reader.null_bitmap();
    assert!(!null_bm.is_empty(), "should have null entries");

    // Non-null entries
    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();

    // All non-null rows
    let all = reader.all_non_null_rows().await.unwrap();
    assert_eq!(all.len(), entries.len() as u64);

    // Null + non-null should cover all rows
    let total = null_bm.len() + entries.len() as u64;
    assert!(total > 0);

    // Point query on a non-null key
    let bm = reader.query_equal(&le_int_key(0)).await.unwrap();
    assert_eq!(bm.len(), 1);

    // Null row ids should not appear in non-null queries
    for null_id in null_bm.iter() {
        assert!(
            !all.contains(null_id),
            "null row_id {null_id} should not be in non-null rows"
        );
    }
}

#[tokio::test]
async fn test_java_compat_varchar_no_compress() {
    let data = load_testdata("btree_varchar_100_no_compress.bin");

    let meta = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"yyyy".to_vec()), false);
    let reader = BTreeIndexReader::new(data, &meta, |a, b| a.cmp(b)).unwrap();

    let entries: Vec<_> = reader.entry_iterator().map(|e| e.unwrap()).collect();
    assert_eq!(entries.len(), 100);

    // Verify sorted order
    for i in 1..entries.len() {
        assert!(entries[i].key > entries[i - 1].key, "keys must be sorted");
    }

    // Verify variable lengths exist
    let lengths: std::collections::HashSet<usize> = entries.iter().map(|e| e.key.len()).collect();
    assert!(lengths.contains(&1));
    assert!(lengths.contains(&2));
    assert!(lengths.contains(&3));
    assert!(lengths.contains(&4));

    // Point query
    let bm = reader.query_equal(b"a").await.unwrap();
    assert_eq!(bm.len(), 1);

    // Each key has exactly one row_id
    for entry in &entries {
        assert_eq!(entry.row_ids.len(), 1);
    }
}

#[test]
fn test_java_compat_int_lz4_unsupported() {
    let data = load_testdata("btree_int_100_lz4.bin");
    let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
    // Reader creation should succeed (only reads footer + index block which may not be compressed)
    // But iterating data blocks should fail with unsupported compression
    let reader = BTreeIndexReader::new(data, &meta, le_int_cmp);
    match reader {
        Err(e) => {
            assert!(
                e.to_string().contains("not supported") || e.to_string().contains("Unsupported"),
                "Expected unsupported compression error, got: {e}"
            );
        }
        Ok(reader) => {
            // If reader creation succeeded, iteration should fail
            let mut iter = reader.entry_iterator();
            let result = iter.next();
            assert!(result.is_some());
            let err = result.unwrap();
            assert!(err.is_err(), "LZ4 data block read should fail");
        }
    }
}
