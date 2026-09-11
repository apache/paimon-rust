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

mod common;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, IntType, Schema, TableSchema, VarCharType, ROW_KIND_FIELD_ID, ROW_KIND_FIELD_NAME,
    SEQUENCE_NUMBER_FIELD_NAME,
};
use paimon::table::{AuditLogTable, IncrementalPlan, IncrementalScanMode, IncrementalSplit};

use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, pk_schema, setup_dirs,
    write_batch,
};

fn collect_audit_rows(batches: &[RecordBatch]) -> Vec<(String, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let kind_idx = schema.index_of("rowkind").unwrap();
        let id_idx = schema.index_of("id").unwrap();
        let value_idx = schema.index_of("value").unwrap();
        let kinds = batch
            .column(kind_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

fn collect_audit_rows_with_sequence(batches: &[RecordBatch]) -> Vec<(String, i64, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let kind_idx = schema.index_of("rowkind").unwrap();
        let seq_idx = schema.index_of(SEQUENCE_NUMBER_FIELD_NAME).unwrap();
        let id_idx = schema.index_of("id").unwrap();
        let value_idx = schema.index_of("value").unwrap();
        let kinds = batch
            .column(kind_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seqs = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                kinds.value(row).to_string(),
                seqs.value(row),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn audit_log_changelog_scan_exposes_rowkind_as_first_column() {
    let table_path = "memory:/audit_log/changelog_rowkind";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(
            vec![1, 1, 2, 2],
            vec![10, 20, 25, 30],
            vec![0, 1, 2, 3],
        ))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    assert_eq!(batches[0].schema().field(0).name(), "rowkind");
    assert_eq!(
        collect_audit_rows(&batches),
        vec![
            ("+I".to_string(), 1, 10),
            ("+U".to_string(), 2, 25),
            ("-D".to_string(), 2, 30),
            ("-U".to_string(), 1, 20),
        ]
    );
}

#[tokio::test]
async fn audit_log_delta_scan_emits_plus_i_for_all_rows() {
    let table_path = "memory:/audit_log/delta_plus_i";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(table_path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    let rowkind_field = batches[0].schema().field(0).clone();
    assert_eq!(rowkind_field.name(), ROW_KIND_FIELD_NAME);
    assert_eq!(rowkind_field.data_type(), &arrow_schema::DataType::Utf8);
    assert!(rowkind_field.is_nullable());
    assert_eq!(
        rowkind_field.metadata().get("PARQUET:field_id"),
        Some(&ROW_KIND_FIELD_ID.to_string())
    );

    assert_eq!(
        collect_audit_rows(&batches),
        vec![("+I".to_string(), 1, 10), ("+I".to_string(), 2, 20),]
    );
}

#[tokio::test]
async fn audit_log_delta_scan_preserves_pk_row_kinds() {
    let table_path = "memory:/audit_log/delta_rowkind";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(
            // Use distinct keys so the PK writer does not merge multiple
            // changes for one key before the audit read sees the data file.
            vec![1, 2, 3, 4],
            vec![10, 20, 25, 30],
            vec![0, 1, 2, 3],
        ))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    assert_eq!(
        collect_audit_rows(&batches),
        vec![
            ("+I".to_string(), 1, 10),
            ("+U".to_string(), 3, 25),
            ("-D".to_string(), 4, 30),
            ("-U".to_string(), 2, 20),
        ]
    );
}

#[test]
fn audit_log_rowkind_field_matches_java_special_field() {
    let (_, table) = memory_table("memory:/audit_log/rowkind_field", pk_schema(&[]));
    let field = AuditLogTable::new(table).fields().unwrap().remove(0);

    assert_eq!(field.id(), ROW_KIND_FIELD_ID);
    assert_eq!(field.name(), ROW_KIND_FIELD_NAME);
    assert!(matches!(
        field.data_type(),
        DataType::VarChar(varchar) if varchar.length() == VarCharType::MAX_LENGTH
    ));
}

#[tokio::test]
async fn audit_log_exposes_sequence_number_when_enabled() {
    let table_path = "memory:/audit_log/sequence_number";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("table-read.sequence-number.enabled", "true"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(vec![1, 1], vec![10, 20], vec![0, 1]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let field_names: Vec<String> = audit
        .fields()
        .unwrap()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(
        field_names,
        vec![
            "rowkind".to_string(),
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            "id".to_string(),
            "value".to_string(),
        ]
    );

    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();
    let batch_schema: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        batch_schema,
        vec![
            "rowkind".to_string(),
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            "id".to_string(),
            "value".to_string(),
        ]
    );

    let rows = collect_audit_rows_with_sequence(&batches);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, seq, _, _)| *seq >= 0));
}

#[tokio::test]
async fn audit_log_current_scan_keeps_delete_and_sequence_number() {
    let table_path = "memory:/audit_log/current_state";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("table-read.sequence-number.enabled", "true"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(vec![1, 2], vec![10, 25], vec![3, 2]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = AuditLogTable::new(table)
        .to_arrow_for_splits(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        collect_audit_rows_with_sequence(&batches),
        vec![("+U".to_string(), 3, 2, 25), ("-D".to_string(), 2, 1, 10),]
    );
}

async fn audit_diff_rows(
    table: &paimon::table::Table,
    start: i64,
    end: i64,
) -> Vec<(String, i32, i32)> {
    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Diff, start, end)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();
    collect_audit_rows(&batches)
}

fn assert_rows_contain(rows: &[(String, i32, i32)], expected: &[(&str, i32, i32)]) {
    for (kind, id, value) in expected {
        assert!(
            rows.iter()
                .any(|(k, i, v)| k == *kind && i == id && v == value),
            "missing rowkind={kind} id={id} value={value} in {rows:?}"
        );
    }
}

fn assert_rows_exclude(rows: &[(String, i32, i32)], excluded: &[(&str, i32, i32)]) {
    for (kind, id, value) in excluded {
        assert!(
            !rows
                .iter()
                .any(|(k, i, v)| k == *kind && i == id && v == value),
            "unexpected rowkind={kind} id={id} value={value} in {rows:?}"
        );
    }
}

#[tokio::test]
async fn audit_log_current_scan_uses_merged_rowkind() {
    for merge_engine in ["partial-update", "aggregation"] {
        let table_path = format!("memory:/audit_log/current_{merge_engine}");
        let (file_io, table) = memory_table(
            &table_path,
            pk_schema(&[("merge-engine", merge_engine), ("bucket", "1")]),
        );
        setup_dirs(&file_io, &table_path).await;
        persist_table_schema(&file_io, &table_path, table.schema()).await;

        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let builder = table.new_write_builder();
        let mut write = builder.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![20], vec![2]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages).await.unwrap();

        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = AuditLogTable::new(table)
            .to_arrow_for_splits(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            collect_audit_rows(&batches),
            vec![("+I".to_string(), 1, 20)],
            "merge-engine={merge_engine}"
        );
    }
}

#[tokio::test]
async fn audit_log_current_scan_respects_ignore_delete() {
    let table_path = "memory:/audit_log/current_ignore_delete";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("merge-engine", "deduplicate"),
            ("ignore-delete", "true"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![10], vec![3]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = AuditLogTable::new(table)
        .to_arrow_for_splits(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        collect_audit_rows(&batches),
        vec![("+I".to_string(), 1, 10)]
    );
}

#[tokio::test]
async fn audit_log_current_scan_supports_first_row() {
    let table_path = "memory:/audit_log/current_first_row";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("merge-engine", "first-row"),
            ("bucket", "1"),
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let mut limited_reader = table.new_read_builder();
    limited_reader.with_limit(1);
    let limited_plan = limited_reader.new_audit_scan().plan().await.unwrap();
    assert_eq!(
        limited_plan.splits().len(),
        2,
        "audit LIMIT must not discard files needed to resolve row versions"
    );

    let audit = AuditLogTable::new(table);
    let plan = audit.new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 2);
    let batches: Vec<RecordBatch> = audit
        .to_arrow_for_splits(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        collect_audit_rows(&batches),
        vec![("+I".to_string(), 1, 10)]
    );
}

#[tokio::test]
async fn audit_log_diff_scan_emits_row_level_delete_insert_and_updates() {
    let table_path = "memory:/audit_log/diff_range";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(&table, &make_batch(vec![2, 3], vec![25, 30])).await;

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_eq!(
        rows,
        vec![
            ("+I".to_string(), 3, 30),
            ("+U".to_string(), 2, 25),
            ("-U".to_string(), 2, 20),
        ]
    );
}

#[tokio::test]
async fn audit_log_diff_same_snapshot_range_returns_no_rows() {
    let table_path = "memory:/audit_log/diff_same_snapshot";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let rows = audit_diff_rows(&table, 1, 1).await;
    assert!(
        rows.is_empty(),
        "same start/end snapshot should yield empty diff"
    );
}

#[tokio::test]
async fn audit_log_diff_insert_only_emits_plus_i() {
    let table_path = "memory:/audit_log/diff_insert_only";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_eq!(rows, vec![("+I".to_string(), 2, 20)]);
}

#[tokio::test]
async fn audit_log_diff_update_only_emits_minus_u_and_plus_u_from_before_after() {
    let table_path = "memory:/audit_log/diff_update_only";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_eq!(
        rows,
        vec![("+U".to_string(), 1, 20), ("-U".to_string(), 1, 10),]
    );
}

#[tokio::test]
async fn audit_log_diff_delete_via_input_delete_row() {
    // Diff compares materialized PK state; input -D removes a key without compact.
    let table_path = "memory:/audit_log/diff_delete_input";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2], vec![10, 20], vec![0, 0]),
    )
    .await;
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![10], vec![3])).await;

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_rows_contain(&rows, &[("-D", 1, 10)]);
    assert_rows_exclude(&rows, &[("+I", 1, 10), ("-U", 1, 10), ("+U", 1, 10)]);
}

#[tokio::test]
async fn audit_log_diff_mixed_delete_insert_update_without_compact() {
    let table_path = "memory:/audit_log/diff_mixed";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2, 3], vec![10, 20, 30], vec![0, 0, 0]),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2, 4], vec![10, 25, 40], vec![3, 2, 0]),
    )
    .await;

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_rows_contain(
        &rows,
        &[("-D", 1, 10), ("-U", 2, 20), ("+U", 2, 25), ("+I", 4, 40)],
    );
    assert_rows_exclude(&rows, &[("+I", 3, 30), ("-D", 3, 30)]);
}

#[tokio::test]
async fn audit_log_diff_processes_multiple_bucket_pairs() {
    let table_path = "memory:/audit_log/diff_multi_bucket";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "4"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 8], vec![10, 80])).await;
    write_batch(&table, &make_batch(vec![1, 8], vec![11, 81])).await;

    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let diff_pairs = plan
        .splits()
        .iter()
        .filter(|split| matches!(split, paimon::table::IncrementalSplit::DiffPair { .. }))
        .count();
    assert!(
        diff_pairs >= 2,
        "expected multiple (partition,bucket) diff pairs, got {diff_pairs}"
    );

    let rows = audit_diff_rows(&table, 1, 2).await;
    assert_rows_contain(
        &rows,
        &[("-U", 1, 10), ("+U", 1, 11), ("-U", 8, 80), ("+U", 8, 81)],
    );
}

#[tokio::test]
async fn audit_log_diff_merges_multiple_splits_per_bucket_by_primary_key() {
    let table_path = "memory:/audit_log/diff_multi_split_bucket";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("target-file-size", "1b"),
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
            ("num-sorted-run.compaction-trigger", "100"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Diff, 2, 3)
        .plan()
        .await
        .unwrap();
    let scrambled = plan
        .splits()
        .iter()
        .cloned()
        .map(|split| match split {
            IncrementalSplit::DiffPair { mut before, after } => {
                assert!(before.len() >= 2, "test requires multiple before splits");
                assert!(after.len() >= 2, "test requires multiple after splits");
                before.reverse();
                IncrementalSplit::DiffPair { before, after }
            }
            other => other,
        })
        .collect();
    let scrambled = IncrementalPlan::try_new(IncrementalScanMode::Diff, scrambled).unwrap();

    let batches: Vec<RecordBatch> = audit
        .to_arrow(&scrambled)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        collect_audit_rows(&batches),
        vec![("+U".to_string(), 1, 11), ("-U".to_string(), 1, 10),]
    );
}

#[tokio::test]
async fn audit_log_diff_with_sequence_number_enabled_exposes_ordered_columns() {
    use std::collections::HashMap;

    let table_path = "memory:/audit_log/diff_sequence";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ])
        .copy_with_options(HashMap::from([(
            "table-read.sequence-number.enabled".to_string(),
            "true".to_string(),
        )])),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let audit = AuditLogTable::new(table.clone());
    let field_names: Vec<String> = audit
        .fields()
        .unwrap()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(
        field_names,
        vec![
            "rowkind".to_string(),
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            "id".to_string(),
            "value".to_string(),
        ]
    );

    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();
    let rows = collect_audit_rows_with_sequence(&batches);
    assert_eq!(rows.len(), 2);
    assert_rows_contain(
        &rows
            .iter()
            .map(|(k, _s, i, v)| (k.clone(), *i, *v))
            .collect::<Vec<_>>(),
        &[("-U", 1, 10), ("+U", 1, 20)],
    );
    assert!(rows.iter().all(|(_, seq, _, _)| *seq >= 0));
}

#[tokio::test]
async fn audit_log_rejects_invalid_incremental_plan_at_consumption() {
    let table_path = "memory:/audit_log/invalid_incremental_plan";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("merge-engine", "deduplicate"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    let audit = AuditLogTable::new(table.clone());

    let invalid_kind = IncrementalPlan::new(
        IncrementalScanMode::Delta,
        vec![IncrementalSplit::DiffPair {
            before: Vec::new(),
            after: Vec::new(),
        }],
    );
    let err = match audit.to_arrow(&invalid_kind) {
        Ok(_) => panic!("invalid plans must fail instead of producing an empty audit stream"),
        Err(err) => err,
    };
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("DiffPair")),
        "invalid plans must fail instead of producing an empty audit stream: {err:?}"
    );

    let auto = IncrementalPlan::new(IncrementalScanMode::Auto, Vec::new());
    let err = match audit.to_arrow(&auto) {
        Ok(_) => panic!("Auto plans must fail at consumption"),
        Err(err) => err,
    };
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("Auto")),
        "Auto plans must fail at consumption: {err:?}"
    );

    let err = IncrementalPlan::try_new(IncrementalScanMode::Auto, Vec::new()).unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("Auto")),
        "try_new must reject unresolved Auto plans: {err:?}"
    );

    let read = table.new_read_builder().new_read().unwrap();
    let err = match read.to_incremental_arrow(&auto) {
        Ok(_) => panic!("the direct incremental reader must validate plans too"),
        Err(err) => err,
    };
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("Auto")),
        "the direct incremental reader must validate plans too: {err:?}"
    );
}
