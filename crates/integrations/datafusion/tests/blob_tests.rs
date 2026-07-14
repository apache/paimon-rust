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

//! Blob type integration tests.
//!
//! Reference: Java Paimon's `BlobTestBase` in paimon-spark.

mod common;

use arrow_array::{Array, BinaryArray, Int32Array, RecordBatch, StringArray};
use common::{assert_sql_error, create_sql_context, create_test_env, exec};
use paimon::catalog::Identifier;
use paimon::spec::{BlobDescriptor, BlobViewStruct};
use paimon::table::BranchManager;
use paimon::Catalog;
use paimon_datafusion::SQLContext;

// ======================= Helpers =======================

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

async fn setup(table_ddl: &str) -> (tempfile::TempDir, SQLContext) {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context.sql(table_ddl).await.unwrap();
    (tmp, sql_context)
}

fn collect_id_name_picture(batches: &[RecordBatch]) -> Vec<(i32, String, Option<Vec<u8>>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pics = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let pic = if pics.is_null(i) {
                None
            } else {
                Some(pics.value(i).to_vec())
            };
            rows.push((ids.value(i), names.value(i).to_string(), pic));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);
    rows
}

async fn query_id_name_picture(
    sql_context: &SQLContext,
    sql: &str,
) -> Vec<(i32, String, Option<Vec<u8>>)> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    collect_id_name_picture(&batches)
}

fn collect_id_name(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

const BLOB_TABLE_DDL: &str = "\
    CREATE TABLE paimon.test_db.t (\
        id INT, \
        name STRING, \
        picture BLOB \
    ) WITH (\
        'data-evolution.enabled' = 'true', \
        'row-tracking.enabled' = 'true'\
    )";

// ======================= Tests =======================

/// Reference: BlobTestBase "Blob: test basic"
#[tokio::test]
async fn test_blob_basic() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Alice', X'48656C6C6F')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "Alice");
    assert_eq!(rows[0].2, Some(b"Hello".to_vec()));
}

/// Reference: BlobTestBase "Blob: test multiple blobs"
#[tokio::test]
async fn test_blob_multiple_columns() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            pic1 BLOB, \
            pic2 BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, pic1, pic2) VALUES (1, X'414141', X'424242')",
    )
    .await;

    let batches = sql_context
        .sql("SELECT id, pic1, pic2 FROM paimon.test_db.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let p1 = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let p2 = batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(p1.value(0), b"AAA");
    assert_eq!(p2.value(0), b"BBB");
}

/// Blob with NULL values.
#[tokio::test]
async fn test_blob_with_nulls() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'48656C6C6F'), \
         (2, 'Bob', NULL), \
         (3, 'Carol', X'576F726C64')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".into(), Some(b"Hello".to_vec())),
            (2, "Bob".into(), None),
            (3, "Carol".into(), Some(b"World".to_vec())),
        ]
    );
}

/// Multiple inserts produce multiple file pairs, all readable.
#[tokio::test]
async fn test_blob_multiple_inserts() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Alice', X'4141')",
    )
    .await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (2, 'Bob', X'4242')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".into(), Some(b"AA".to_vec())),
            (2, "Bob".into(), Some(b"BB".to_vec())),
        ]
    );
}

/// blob-descriptor-field: listed fields are stored inline in parquet (no .blob files).
#[tokio::test]
async fn test_blob_descriptor_field_inline() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Alice', X'48656C6C6F')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "Alice");
    assert_eq!(rows[0].2, Some(b"Hello".to_vec()));
}

/// MERGE INTO on a raw-blob table: updating a non-blob column should succeed,
/// and the blob data should be preserved.
#[tokio::test]
async fn test_merge_into_updates_non_blob_on_raw_blob_table() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'4141'), \
         (2, 'Bob', X'4242')",
    )
    .await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (1, 'Updated')) AS t(id, name)",
    )
    .await;

    exec(
        &sql_context,
        "MERGE INTO paimon.test_db.t t \
         USING paimon.test_db.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Updated".into(), Some(b"AA".to_vec())),
            (2, "Bob".into(), Some(b"BB".to_vec())),
        ]
    );
}

/// Reference: BlobTestBase "Blob: merge-into rejects updating raw-data BLOB column"
#[tokio::test]
async fn test_merge_into_rejects_raw_blob_update() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Alice', X'4141')",
    )
    .await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (1, X'4242')) AS t(id, picture)",
    )
    .await;

    let result = sql_context
        .sql(
            "MERGE INTO paimon.test_db.t t \
             USING paimon.test_db.src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET picture = s.picture",
        )
        .await;

    assert!(
        result.is_err() || {
            let df = result.unwrap();
            df.collect().await.is_err()
        }
    );
}

/// Reference: BlobTestBase "Blob: merge-into updates non-blob column on descriptor blob table"
#[tokio::test]
async fn test_merge_into_updates_non_blob_on_descriptor_table() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'4141'), \
         (2, 'Bob', X'4242')",
    )
    .await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (1, 'Updated')) AS t(id, name)",
    )
    .await;

    exec(
        &sql_context,
        "MERGE INTO paimon.test_db.t t \
         USING paimon.test_db.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;

    let batches = sql_context
        .sql("SELECT id, name FROM paimon.test_db.t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = collect_id_name(&batches);
    assert_eq!(rows, vec![(1, "Updated".into()), (2, "Bob".into())]);
}

/// Merge-into on a descriptor blob table: updating the blob column should succeed.
#[tokio::test]
async fn test_merge_into_updates_blob_on_descriptor_table() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'4141'), \
         (2, 'Bob', X'4242')",
    )
    .await;

    exec(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS SELECT * FROM (VALUES (1, X'4343')) AS t(id, picture)",
    )
    .await;

    exec(
        &sql_context,
        "MERGE INTO paimon.test_db.t t \
         USING paimon.test_db.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET picture = s.picture",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".into(), Some(b"CC".to_vec())),
            (2, "Bob".into(), Some(b"BB".to_vec())),
        ]
    );
}

/// Blob with partitioned table.
#[tokio::test]
async fn test_blob_with_partition() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            picture BLOB, \
            pt STRING \
         ) PARTITIONED BY (pt) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, picture, pt) VALUES \
         (1, X'4141', 'a'), \
         (2, X'4242', 'b')",
    )
    .await;

    let batches = sql_context
        .sql("SELECT id, picture FROM paimon.test_db.t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let pics = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), pics.value(i).to_vec()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(rows, vec![(1, b"AA".to_vec()), (2, b"BB".to_vec())]);
}

/// When a blob column value is a serialized BlobDescriptor, the writer should
/// resolve it by reading actual data from the referenced URI.
#[tokio::test]
async fn test_blob_resolve_descriptor() {
    let (tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    // Write a source file that the BlobDescriptor will reference.
    let source_data = b"ResolvedBlobContent";
    let source_path = tmp.path().join("blob_source.bin");
    std::fs::write(&source_path, source_data).unwrap();

    let uri = format!("file://{}", source_path.display());
    let desc = BlobDescriptor::new(uri, 0, source_data.len() as i64);
    let desc_hex = to_hex(&desc.serialize());

    // Insert: row 1 has a BlobDescriptor value, row 2 has raw data.
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Descriptor', X'{desc_hex}'), \
         (2, 'Raw', X'48656C6C6F')"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    // Descriptor was resolved to actual file content
    assert_eq!(
        rows[0],
        (1, "Descriptor".into(), Some(source_data.to_vec()))
    );
    // Raw data passed through unchanged
    assert_eq!(rows[1], (2, "Raw".into(), Some(b"Hello".to_vec())));
}

/// BlobDescriptor with non-zero offset and partial length.
#[tokio::test]
async fn test_blob_resolve_descriptor_with_offset() {
    let (tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    let source_data = b"HEADER_PAYLOAD_TRAILER";
    let source_path = tmp.path().join("blob_offset.bin");
    std::fs::write(&source_path, source_data).unwrap();

    // Reference only "PAYLOAD" (offset=7, length=7)
    let uri = format!("file://{}", source_path.display());
    let desc = BlobDescriptor::new(uri, 7, 7);
    let desc_hex = to_hex(&desc.serialize());

    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Partial', X'{desc_hex}')"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Partial".into(), Some(b"PAYLOAD".to_vec())));
}

#[tokio::test]
async fn test_blob_resolve_unknown_length_descriptor() {
    let (tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    let source_data = b"HEADER_PAYLOAD_TRAILER";
    let source_path = tmp.path().join("blob_unknown_length.bin");
    std::fs::write(&source_path, source_data).unwrap();

    let uri = format!("file://{}", source_path.display());
    let full_hex = to_hex(&BlobDescriptor::new(uri.clone(), 0, -1).serialize());
    let suffix_hex = to_hex(&BlobDescriptor::new(uri.clone(), 7, -1).serialize());
    let eof_hex =
        to_hex(&BlobDescriptor::new(uri.clone(), source_data.len() as i64, -1).serialize());
    let past_eof_hex =
        to_hex(&BlobDescriptor::new(uri, source_data.len() as i64 + 5, -1).serialize());

    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Full', X'{full_hex}'), \
         (2, 'Suffix', X'{suffix_hex}'), \
         (3, 'Eof', X'{eof_hex}'), \
         (4, 'PastEof', X'{past_eof_hex}'), \
         (5, 'Raw', X'524157'), \
         (6, 'Null', NULL)"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Full".into(), Some(source_data.to_vec())),
            (2, "Suffix".into(), Some(b"PAYLOAD_TRAILER".to_vec())),
            (3, "Eof".into(), Some(Vec::new())),
            (4, "PastEof".into(), Some(Vec::new())),
            (5, "Raw".into(), Some(b"RAW".to_vec())),
            (6, "Null".into(), None),
        ]
    );
}

#[tokio::test]
async fn test_blob_descriptor_short_read_returns_error() {
    let (tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    let source_path = tmp.path().join("blob_short_read.bin");
    std::fs::write(&source_path, b"short").unwrap();
    let uri = format!("file://{}", source_path.display());
    let desc_hex = to_hex(&BlobDescriptor::new(uri, 0, 6).serialize());
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Short', X'{desc_hex}')"
    );

    assert_sql_error(&sql_context, &sql, "Failed to read BlobDescriptor").await;
}

/// Blob files roll independently when `blob.target-file-size` is small.
#[tokio::test]
async fn test_blob_rolling() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob.target-file-size' = '50'\
         )",
    )
    .await;

    // Insert multiple rows with blob data large enough to trigger rolling at 50 bytes.
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'A', X'4141414141414141414141414141414141414141414141414141414141414141'), \
         (2, 'B', X'4242424242424242424242424242424242424242424242424242424242424242'), \
         (3, 'C', X'4343434343434343434343434343434343434343434343434343434343434343')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "A".into(), Some(vec![0x41; 32])));
    assert_eq!(rows[1], (2, "B".into(), Some(vec![0x42; 32])));
    assert_eq!(rows[2], (3, "C".into(), Some(vec![0x43; 32])));
}

/// blob-descriptor-field with multiple inserts: descriptor values are resolved to actual data.
#[tokio::test]
async fn test_blob_descriptor_field_resolve_on_read() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'48656C6C6F'), \
         (2, 'Bob', NULL), \
         (3, 'Carol', X'576F726C64')",
    )
    .await;

    // Second insert to exercise multi-file merge path.
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (4, 'Dave', X'5061696D6F6E')",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".into(), Some(b"Hello".to_vec())),
            (2, "Bob".into(), None),
            (3, "Carol".into(), Some(b"World".to_vec())),
            (4, "Dave".into(), Some(b"Paimon".to_vec())),
        ]
    );
}

/// blob-descriptor-field: inserting a serialized BlobDescriptor resolves to actual data on read.
#[tokio::test]
async fn test_blob_descriptor_field_resolve_descriptor_value() {
    let (tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    // Write a source file that the BlobDescriptor will reference.
    let source_data = b"DescriptorResolved";
    let source_path = tmp.path().join("desc_source.bin");
    std::fs::write(&source_path, source_data).unwrap();

    let uri = format!("file://{}", source_path.display());
    let desc = BlobDescriptor::new(uri, 0, source_data.len() as i64);
    let desc_hex = to_hex(&desc.serialize());

    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'FromDesc', X'{desc_hex}'), \
         (2, 'Raw', X'48656C6C6F')"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "FromDesc".into(), Some(b"DescriptorResolved".to_vec())),
            (2, "Raw".into(), Some(b"Hello".to_vec())),
        ]
    );
}

#[tokio::test]
async fn test_blob_descriptor_field_resolve_unknown_length_descriptor() {
    let (tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    let source_data = b"HEADER_PAYLOAD_TRAILER";
    let source_path = tmp.path().join("descriptor_unknown_length.bin");
    std::fs::write(&source_path, source_data).unwrap();

    let uri = format!("file://{}", source_path.display());
    let bounded_hex = to_hex(&BlobDescriptor::new(uri.clone(), 0, 6).serialize());
    let suffix_hex = to_hex(&BlobDescriptor::new(uri.clone(), 7, -1).serialize());
    let eof_hex =
        to_hex(&BlobDescriptor::new(uri.clone(), source_data.len() as i64, -1).serialize());
    let past_eof_hex =
        to_hex(&BlobDescriptor::new(uri, source_data.len() as i64 + 5, -1).serialize());
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Bounded', X'{bounded_hex}'), \
         (2, 'Suffix', X'{suffix_hex}'), \
         (3, 'Eof', X'{eof_hex}'), \
         (4, 'PastEof', X'{past_eof_hex}'), \
         (5, 'Raw', X'524157'), \
         (6, 'Null', NULL)"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "Bounded".into(), Some(b"HEADER".to_vec())),
            (2, "Suffix".into(), Some(b"PAYLOAD_TRAILER".to_vec())),
            (3, "Eof".into(), Some(Vec::new())),
            (4, "PastEof".into(), Some(Vec::new())),
            (5, "Raw".into(), Some(b"RAW".to_vec())),
            (6, "Null".into(), None),
        ]
    );
}

#[tokio::test]
async fn test_blob_descriptor_field_rejects_invalid_length() {
    let (tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    let uri = format!("file://{}", tmp.path().join("unused.bin").display());
    let desc_hex = to_hex(&BlobDescriptor::new(uri, 0, -2).serialize());
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Invalid', X'{desc_hex}')"
    );
    exec(&sql_context, &sql).await;

    assert_sql_error(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t",
        "length must be -1 or non-negative",
    )
    .await;
}

#[tokio::test]
async fn test_blob_descriptor_field_short_read_returns_error() {
    let (tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    let source_path = tmp.path().join("descriptor_short_read.bin");
    std::fs::write(&source_path, b"short").unwrap();
    let uri = format!("file://{}", source_path.display());
    let desc_hex = to_hex(&BlobDescriptor::new(uri, 0, 6).serialize());
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES (1, 'Short', X'{desc_hex}')"
    );
    exec(&sql_context, &sql).await;

    assert_sql_error(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t",
        "Failed to read BlobDescriptor",
    )
    .await;
}

#[tokio::test]
async fn test_blob_descriptor_filter_before_resolve_skips_filtered_bad_descriptor() {
    let (tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.t (\
            id INT, \
            name STRING, \
            picture BLOB \
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true', \
            'blob-descriptor-field' = 'picture'\
         )",
    )
    .await;

    let missing_uri = format!("file://{}", tmp.path().join("missing_blob.bin").display());
    let bad_desc = BlobDescriptor::new(missing_uri, 0, 1);
    let bad_desc_hex = to_hex(&bad_desc.serialize());
    let sql = format!(
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Filtered', X'{bad_desc_hex}'), \
         (2, 'Kept', X'4F4B')"
    );
    exec(&sql_context, &sql).await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t WHERE id = 2",
    )
    .await;
    assert_eq!(rows, vec![(2, "Kept".into(), Some(b"OK".to_vec()))]);
}

/// SET 'paimon.blob-as-descriptor' = 'true' should return serialized BlobDescriptor
/// bytes instead of the actual blob content.
#[tokio::test]
async fn test_blob_as_descriptor_dynamic_option() {
    let (_tmp, sql_context) = setup(BLOB_TABLE_DDL).await;

    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.t (id, name, picture) VALUES \
         (1, 'Alice', X'48656C6C6F'), \
         (2, 'Bob', X'576F726C64')",
    )
    .await;

    // Without the option, we get raw blob data.
    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows[0].2, Some(b"Hello".to_vec()));

    // Enable blob-as-descriptor via dynamic option.
    sql_context
        .sql("SET 'paimon.blob-as-descriptor' = 'true'")
        .await
        .unwrap();

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);

    // The returned bytes should be valid BlobDescriptors, not raw data.
    let desc_bytes = rows[0].2.as_ref().expect("expected descriptor bytes");
    assert!(
        BlobDescriptor::is_blob_descriptor(desc_bytes),
        "expected BlobDescriptor, got raw data"
    );
    let desc = BlobDescriptor::deserialize(desc_bytes).expect("failed to deserialize descriptor");
    assert!(desc.uri().starts_with("file://"), "uri: {}", desc.uri());
    assert!(desc.length() > 0);

    // RESET should go back to raw data.
    sql_context
        .sql("RESET 'paimon.blob-as-descriptor'")
        .await
        .unwrap();

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.t ORDER BY id",
    )
    .await;
    assert_eq!(rows[0].2, Some(b"Hello".to_vec()));
    assert_eq!(rows[1].2, Some(b"World".to_vec()));
}

#[tokio::test]
async fn test_blob_view_without_rest_env_preserves_reference() {
    let (tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.src (\
                id INT, \
                name STRING, \
                picture BLOB\
             ) WITH (\
                'data-evolution.enabled' = 'true', \
                'row-tracking.enabled' = 'true'\
             )",
        )
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.src (id, name, picture) VALUES \
         (1, 'Alice', X'616C696365'), \
         (2, 'Bob', X'626F62')",
    )
    .await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.view_t (\
                id INT, \
                name STRING, \
                picture BLOB\
             ) WITH (\
                'data-evolution.enabled' = 'true', \
                'row-tracking.enabled' = 'true', \
                'blob-view-field' = 'picture'\
             )",
        )
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.view_t (id, name, picture) \
         SELECT id, name, sys.blob_view('test_db.src', 'picture', \"_ROW_ID\") \
         FROM paimon.test_db.src",
    )
    .await;

    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.view_t ORDER BY id",
    )
    .await;
    assert_eq!(rows[0].1, "Alice");
    assert_eq!(rows[1].1, "Bob");
    let view0_bytes = rows[0].2.as_ref().expect("view bytes should be preserved");
    let view1_bytes = rows[1].2.as_ref().expect("view bytes should be preserved");
    assert!(BlobViewStruct::is_blob_view_struct(view0_bytes));
    assert!(BlobViewStruct::is_blob_view_struct(view1_bytes));
    let view0 = BlobViewStruct::deserialize(view0_bytes).unwrap();
    let view1 = BlobViewStruct::deserialize(view1_bytes).unwrap();
    assert_eq!(view0.identifier().full_name(), "test_db.src");
    assert_eq!(view1.identifier().full_name(), "test_db.src");
    assert_eq!(view0.row_id(), 0);
    assert_eq!(view1.row_id(), 1);

    drop(tmp);
}

#[tokio::test]
async fn test_blob_view_preserves_branch_reference() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.src (\
                id INT, \
                picture BLOB\
             ) WITH (\
                'data-evolution.enabled' = 'true', \
                'row-tracking.enabled' = 'true'\
             )",
        )
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.src (id, picture) VALUES (1, X'616C696365')",
    )
    .await;

    let table = catalog
        .get_table(&Identifier::new("test_db", "src"))
        .await
        .expect("load table");
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .expect("load latest snapshot")
        .expect("latest snapshot");
    table
        .tag_manager()
        .create("branch-source", &snapshot)
        .await
        .expect("create tag");
    BranchManager::new(table.file_io().clone(), table.location().to_string())
        .create_branch_from_tag("b1", "branch-source")
        .await
        .expect("create branch");

    let batches = sql_context
        .sql(
            "SELECT blob_view('test_db.src$branch_b1', 'picture', \"_ROW_ID\") AS picture \
             FROM paimon.test_db.src",
        )
        .await
        .expect("blob_view SQL should parse")
        .collect()
        .await
        .expect("branch blob_view should execute");
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary blob view")
        .value(0);
    let view = BlobViewStruct::deserialize(value).expect("deserialize blob view");
    assert_eq!(view.identifier().full_name(), "test_db.src$branch_b1");
    assert_eq!(view.row_id(), 0);
}

#[tokio::test]
async fn test_blob_view_resolve_disabled_preserves_reference() {
    let (_tmp, sql_context) = setup(
        "CREATE TABLE paimon.test_db.src (\
            id INT, \
            name STRING, \
            picture BLOB\
         ) WITH (\
            'data-evolution.enabled' = 'true', \
            'row-tracking.enabled' = 'true'\
         )",
    )
    .await;
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.src (id, name, picture) VALUES (1, 'Alice', X'616C696365')",
    )
    .await;
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.view_t (\
                id INT, \
                name STRING, \
                picture BLOB\
             ) WITH (\
                'data-evolution.enabled' = 'true', \
                'row-tracking.enabled' = 'true', \
                'blob-view-field' = 'picture'\
             )",
        )
        .await
        .unwrap();
    exec(
        &sql_context,
        "INSERT INTO paimon.test_db.view_t (id, name, picture) \
         SELECT id, name, blob_view('test_db.src', 'picture', \"_ROW_ID\") \
         FROM paimon.test_db.src",
    )
    .await;

    sql_context
        .sql("SET 'paimon.blob-view.resolve.enabled' = 'false'")
        .await
        .unwrap();
    let rows = query_id_name_picture(
        &sql_context,
        "SELECT id, name, picture FROM paimon.test_db.view_t ORDER BY id",
    )
    .await;
    let view_bytes = rows[0].2.as_ref().expect("view bytes should be preserved");
    assert!(BlobViewStruct::is_blob_view_struct(view_bytes));
    let view = BlobViewStruct::deserialize(view_bytes).unwrap();
    assert_eq!(view.identifier().full_name(), "test_db.src");
    assert_eq!(view.row_id(), 0);
}
