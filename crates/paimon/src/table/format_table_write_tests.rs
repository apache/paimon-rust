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

//! Format Table write behavior is tested through the same public builder,
//! scan and read calls used by native callers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;

use super::Table;
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{DataType, IntType, Schema, TableSchema, VarCharType};

fn table(io: FileIO, location: &str, partitioned: bool, options: &[(&str, &str)]) -> Table {
    let mut builder = Schema::builder();
    if partitioned {
        builder = builder.column("dt", DataType::VarChar(VarCharType::string_type()));
    }
    builder = builder
        .column("id", DataType::Int(IntType::new()))
        .option("type", "format-table")
        .option("file.format", "parquet");
    if partitioned {
        builder = builder.partition_keys(["dt"]);
    }
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    Table::new(
        io,
        Identifier::new("default", "format_write"),
        location.to_string(),
        TableSchema::new(0, &builder.build().unwrap()),
        None,
    )
}

fn memory_table(name: &str, partitioned: bool, options: &[(&str, &str)]) -> Table {
    table(
        FileIOBuilder::new("memory").build().unwrap(),
        &format!("memory:/{name}"),
        partitioned,
        options,
    )
}

fn two_partition_table(name: &str) -> Table {
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .column("hour", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["dt", "hour"])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_two_partition"),
        format!("memory:/{name}"),
        TableSchema::new(0, &schema),
        None,
    )
}

fn two_partition_batch(rows: &[(&str, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("hour", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn batch(rows: &[(&str, i32)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("dt", ArrowType::Utf8, true),
        Field::new("id", ArrowType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn unpartitioned_batch(ids: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowType::Int32,
            true,
        )])),
        vec![Arc::new(Int32Array::from(ids.to_vec()))],
    )
    .unwrap()
}

async fn ids(table: &Table) -> Vec<i32> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of("id").unwrap();
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend(column.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

async fn visible_files(table: &Table, partition: &str) -> Vec<String> {
    let path = if partition.is_empty() {
        table.location().to_string()
    } else {
        format!("{}/{partition}", table.location())
    };
    let mut files =
        super::format_table_scan::list_format_table_files(table.file_io(), &path, 0, None)
            .await
            .unwrap()
            .into_iter()
            .filter(|status| !status.is_dir)
            .map(|status| status.path)
            .collect::<Vec<_>>();
    files.sort();
    files
}

async fn append(table: &Table, batch: &RecordBatch) {
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

#[tokio::test]
async fn append_partitioned_table_writes_data_without_partition_column() {
    let table = memory_table("format_append_partitioned", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 1), ("b", 2), ("a", 3)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    for message in &messages {
        let file = message.format_file.as_ref().unwrap();
        assert!(file.staged_path.contains("/_temporary/"));
        assert!(!table.file_io().exists(&file.target_path).await.unwrap());
        assert_eq!(file.partition.len(), 1);
    }
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(ids(&table).await, [1, 2, 3]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=b").await.len(), 1);
}

#[tokio::test]
async fn append_unpartitioned_table_has_no_snapshot() {
    let table = memory_table("format_append_plain", false, &[]);
    append(&table, &unpartitioned_batch(&[4, 5])).await;
    append(&table, &unpartitioned_batch(&[6])).await;
    assert_eq!(ids(&table).await, [4, 5, 6]);
    assert_eq!(visible_files(&table, "").await.len(), 2);
    assert!(table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn file_rolls_at_target_row_count_even_inside_one_batch() {
    let table = memory_table("format_roll_rows", true, &[("target-file-row-num", "2")]);
    append(
        &table,
        &batch(&[("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 5)]),
    )
    .await;
    assert_eq!(ids(&table).await, [1, 2, 3, 4, 5]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 3);
}

#[tokio::test]
async fn overwrite_replaces_only_written_partitions_by_default() {
    let table = memory_table("format_overwrite_dynamic", true, &[]);
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 7)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder
        .new_commit()
        .overwrite(messages, None)
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [2, 7]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
}

#[tokio::test]
async fn static_partition_overwrite_without_output_clears_partition() {
    let table = memory_table("format_overwrite_empty", true, &[]);
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    builder
        .new_commit()
        .overwrite(
            Vec::new(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [2]);
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn prepared_files_are_hidden_until_commit_and_abort_discards_them() {
    let table = memory_table("format_abort_prepared", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 1), ("a", 2)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    let staged = &messages[0].format_file.as_ref().unwrap().staged_path;
    assert!(table.file_io().exists(staged).await.unwrap());
    assert!(visible_files(&table, "dt=a").await.is_empty());
    assert!(table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .is_empty());
    builder.new_commit().abort(&messages).await.unwrap();
    assert!(!table.file_io().exists(staged).await.unwrap());
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn closing_a_writer_discards_pending_data_without_publishing() {
    let table = memory_table("format_close_pending", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 1)])).await.unwrap();
    write.close().await;
    assert!(visible_files(&table, "dt=a").await.is_empty());
    assert!(table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .is_empty());
}

#[tokio::test]
async fn rejected_batch_does_not_stage_or_publish_a_file() {
    let table = memory_table("format_rejected_batch", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    let wrong = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["invalid"])),
        ],
    )
    .unwrap();
    let error = write.write_arrow_batch(&wrong).await.err().unwrap();
    assert!(error.to_string().contains("expects"));
    assert!(write.prepare_commit().await.is_err());
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn one_writer_can_prepare_more_than_one_append() {
    let table = memory_table("format_writer_reuse", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 1)])).await.unwrap();
    builder
        .new_commit()
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 2), ("b", 3)]))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [1, 2, 3]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 2);
    assert_eq!(visible_files(&table, "dt=b").await.len(), 1);
}

#[tokio::test]
async fn value_only_partition_path_is_written_and_read() {
    let table = memory_table(
        "format_value_only_write",
        true,
        &[("format-table.partition-path-only-value", "true")],
    );
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    assert_eq!(ids(&table).await, [1, 2]);
    assert_eq!(visible_files(&table, "a").await.len(), 1);
    assert_eq!(visible_files(&table, "b").await.len(), 1);
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn overwrite_without_dynamic_partition_mode_replaces_all_partitions() {
    let table = memory_table(
        "format_overwrite_all",
        true,
        &[("dynamic-partition-overwrite", "false")],
    );
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 7)])).await.unwrap();
    builder
        .new_commit()
        .overwrite(write.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [7]);
    assert!(visible_files(&table, "dt=b").await.is_empty());
}

#[tokio::test]
async fn target_collision_is_rejected_before_any_overwrite_cleanup() {
    let table = memory_table("format_target_collision", true, &[]);
    append(&table, &batch(&[("a", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 2)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let file = messages[0].format_file.as_ref().unwrap();
    table
        .file_io()
        .copy_file_streaming(&file.staged_path, &file.target_path)
        .await
        .unwrap();
    let old_files = visible_files(&table, "dt=a").await;
    let error = builder
        .new_commit()
        .overwrite(messages.clone(), None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("target already exists"));
    assert_eq!(visible_files(&table, "dt=a").await, old_files);
    builder.new_commit().abort(&messages).await.unwrap();
    assert_eq!(visible_files(&table, "dt=a").await, old_files);
}

#[tokio::test]
async fn missing_staged_file_cannot_erase_an_overwritten_partition() {
    let table = memory_table("format_missing_stage", true, &[]);
    append(&table, &batch(&[("a", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 2)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let staged = &messages[0].format_file.as_ref().unwrap().staged_path;
    table.file_io().delete_file(staged).await.unwrap();
    let error = builder
        .new_commit()
        .overwrite(messages, None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("is missing"));
    assert_eq!(ids(&table).await, [1]);
}

#[tokio::test]
async fn format_file_message_refuses_snapshot_wire_serialization() {
    let table = memory_table("format_message_serialize", false, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&unpartitioned_batch(&[1]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let error = messages[0].serialize().err().unwrap();
    assert!(error.to_string().contains("different Java serializer"));
    builder.new_commit().abort(&messages).await.unwrap();
}

#[tokio::test]
async fn data_file_name_honors_prefix_and_compression_suffix() {
    let table = memory_table(
        "format_file_name",
        true,
        &[
            ("data-file.prefix", "event-"),
            ("file.suffix.include.compression", "true"),
        ],
    );
    append(&table, &batch(&[("a", 1)])).await;
    let files = visible_files(&table, "dt=a").await;
    assert_eq!(files.len(), 1);
    let name = files[0].rsplit('/').next().unwrap();
    assert!(name.starts_with("event-"), "{name}");
    assert!(name.ends_with(".snappy.parquet"), "{name}");
    assert_eq!(ids(&table).await, [1]);
}

#[tokio::test]
async fn static_prefix_overwrite_removes_only_matching_descendants() {
    let table = two_partition_table("format_static_prefix");
    append(
        &table,
        &two_partition_batch(&[("a", "00", 1), ("a", "01", 2), ("b", "00", 3)]),
    )
    .await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&two_partition_batch(&[("a", "01", 7)]))
        .await
        .unwrap();
    builder
        .new_commit()
        .overwrite(
            write.prepare_commit().await.unwrap(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [3, 7]);
    assert!(visible_files(&table, "dt=a/hour=00").await.is_empty());
    assert_eq!(visible_files(&table, "dt=a/hour=01").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=b/hour=00").await.len(), 1);
}

#[tokio::test]
async fn empty_static_overwrite_creates_the_selected_partition_directory() {
    let table = two_partition_table("format_static_empty");
    let prefix = HashMap::from([
        (
            "dt".to_string(),
            Some(crate::spec::Datum::String("new".into())),
        ),
        (
            "hour".to_string(),
            Some(crate::spec::Datum::String("09".into())),
        ),
    ]);
    table
        .new_write_builder()
        .with_overwrite()
        .new_commit()
        .overwrite(Vec::new(), Some(prefix))
        .await
        .unwrap();
    assert!(table
        .file_io()
        .exists_dir("memory:/format_static_empty/dt=new/hour=09")
        .await
        .unwrap());
    assert!(ids(&table).await.is_empty());
}

#[tokio::test]
async fn output_outside_static_prefix_is_rejected_before_deleting_old_files() {
    let table = two_partition_table("format_static_mismatch");
    append(&table, &two_partition_batch(&[("a", "00", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&two_partition_batch(&[("b", "00", 2)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let error = builder
        .new_commit()
        .overwrite(
            messages.clone(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("outside the static overwrite"));
    assert_eq!(ids(&table).await, [1]);
    builder.new_commit().abort(&messages).await.unwrap();
}

#[tokio::test]
async fn static_partition_must_be_a_leading_prefix() {
    let table = two_partition_table("format_static_gap");
    let error = table
        .new_write_builder()
        .with_overwrite()
        .new_commit()
        .overwrite(
            Vec::new(),
            Some(HashMap::from([(
                "hour".to_string(),
                Some(crate::spec::Datum::String("00".into())),
            )])),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("lacks its leading partition"));
    assert!(ids(&table).await.is_empty());
}

#[tokio::test]
async fn physical_parquet_file_omits_partition_columns() {
    let table = two_partition_table("format_physical_columns");
    append(&table, &two_partition_batch(&[("a", "00", 7)])).await;
    let files = visible_files(&table, "dt=a/hour=00").await;
    assert_eq!(files.len(), 1);
    let bytes = table
        .file_io()
        .new_input(&files[0])
        .unwrap()
        .read()
        .await
        .unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let fields = reader.schema().fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "id");
    assert_eq!(ids(&table).await, [7]);
}

#[tokio::test]
async fn row_format_round_trips_through_native_read() {
    for format in ["row"] {
        let name = format!("format_write_{format}");
        let table = memory_table(&name, true, &[("file.format", format)]);
        append(&table, &batch(&[("a", 1), ("b", 2), ("a", 3)])).await;
        assert_eq!(ids(&table).await, [1, 2, 3], "format={format}");
        assert_eq!(
            visible_files(&table, "dt=a")
                .await
                .iter()
                .filter(|path| path.ends_with(&format!(".{format}")))
                .count(),
            1,
            "format={format}"
        );
    }
}

#[tokio::test]
async fn null_partition_uses_the_configured_default_directory() {
    let table = memory_table(
        "format_null_partition",
        true,
        &[("partition.default-name", "_empty_")],
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>, Some("a")])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    append(&table, &batch).await;
    assert_eq!(ids(&table).await, [1, 2]);
    assert_eq!(visible_files(&table, "dt=_empty_").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
}

#[test]
fn invalid_target_row_count_is_rejected_before_a_writer_is_opened() {
    let table = memory_table(
        "format_bad_row_target",
        false,
        &[("target-file-row-num", "0")],
    );
    let error = table.new_write_builder().new_write().err().unwrap();
    assert!(error
        .to_string()
        .contains("target-file-row-num must be positive"));
}

#[test]
fn unsupported_format_is_rejected_before_a_writer_is_opened() {
    let table = memory_table(
        "format_unsupported_write",
        false,
        &[("file.format", "json")],
    );
    let error = table.new_write_builder().new_write().err().unwrap();
    assert!(error.to_string().contains("not supported"));
}

#[test]
fn readable_but_unwritable_formats_are_rejected_before_staging() {
    for format in ["avro", "orc", "mosaic"] {
        let table = memory_table(
            &format!("format_no_{format}_writer"),
            false,
            &[("file.format", format)],
        );
        let error = table.new_write_builder().new_write().err().unwrap();
        assert!(
            error
                .to_string()
                .contains("can be read but cannot be written"),
            "format={format}, error={error}"
        );
    }
}
