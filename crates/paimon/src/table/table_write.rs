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

//! TableWrite for writing Arrow data to Paimon tables.
//!
//! Reference: [pypaimon TableWrite](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/table_write.py)
//! and [pypaimon FileStoreWrite](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/file_store_write.py)

use crate::spec::DataFileMeta;
use crate::spec::PartitionComputer;
use crate::spec::{
    BinaryRow, CoreOptions, DataField, DataType, Datum, MergeEngine, Predicate, PredicateBuilder,
    EMPTY_SERIALIZED_ROW, POSTPONE_BUCKET,
};
use crate::table::bucket_assigner::{BucketAssignerEnum, PartitionBucketKey};
use crate::table::bucket_assigner_constant::ConstantBucketAssigner;
use crate::table::bucket_assigner_cross::CrossPartitionAssigner;
use crate::table::bucket_assigner_dynamic::DynamicBucketAssigner;
use crate::table::bucket_assigner_fixed::FixedBucketAssigner;
use crate::table::commit_message::CommitMessage;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::kv_file_writer::{KeyValueFileWriter, KeyValueWriteConfig};
use crate::table::postpone_file_writer::{PostponeFileWriter, PostponeWriteConfig};
use crate::table::{SnapshotManager, Table, TableScan};
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

fn schema_contains_blob_type(fields: &[DataField]) -> bool {
    fields
        .iter()
        .any(|field| field.data_type().contains_blob_type())
}

/// Enum to hold either an append-only writer, a key-value writer, or a postpone writer.
enum FileWriter {
    Append(DataFileWriter),
    KeyValue(KeyValueFileWriter),
    Postpone(PostponeFileWriter),
}

impl FileWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            FileWriter::Append(w) => w.write(batch).await,
            FileWriter::KeyValue(w) => w.write(batch).await,
            FileWriter::Postpone(w) => w.write(batch).await,
        }
    }

    async fn prepare_commit(mut self) -> Result<Vec<DataFileMeta>> {
        match self {
            FileWriter::Append(ref mut w) => w.prepare_commit().await,
            FileWriter::KeyValue(ref mut w) => w.prepare_commit().await,
            FileWriter::Postpone(ref mut w) => w.prepare_commit().await,
        }
    }
}

/// TableWrite writes Arrow RecordBatches to Paimon data files.
///
/// Each (partition, bucket) pair gets its own writer held in a HashMap.
/// Batches are routed to the correct writer based on partition/bucket.
///
/// Call `prepare_commit()` to close all writers and collect
/// `CommitMessage`s for use with `TableCommit`.
///
/// Reference: [pypaimon BatchTableWrite](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/table_write.py)
pub struct TableWrite {
    table: Table,
    partition_writers: HashMap<PartitionBucketKey, FileWriter>,
    partition_computer: PartitionComputer,
    partition_keys: Vec<String>,
    schema_id: i64,
    target_file_size: i64,
    file_compression: String,
    file_compression_zstd_level: i32,
    write_buffer_size: i64,
    primary_key_indices: Vec<usize>,
    primary_key_types: Vec<DataType>,
    sequence_field_indices: Vec<usize>,
    merge_engine: MergeEngine,
    partition_seq_cache: HashMap<Vec<u8>, HashMap<i32, i64>>,
    commit_user: String,
    /// Bucket assignment strategy (fixed, dynamic, or cross-partition).
    bucket_assigner: BucketAssignerEnum,
    /// Whether this is an overwrite operation (skip seq/index restore).
    is_overwrite: bool,
}

impl TableWrite {
    pub(crate) fn new(
        table: &Table,
        commit_user: String,
        is_overwrite: bool,
    ) -> crate::Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if schema_contains_blob_type(schema.fields()) {
            return Err(crate::Error::Unsupported {
                message:
                    "TableWrite does not support BlobType yet; blob write path is out of scope"
                        .to_string(),
            });
        }

        if core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "TableWrite does not support data-evolution.enabled mode".to_string(),
            });
        }

        let total_buckets = core_options.bucket();
        let has_primary_keys = !schema.primary_keys().is_empty();
        let is_dynamic_bucket = has_primary_keys && total_buckets == -1;

        let is_cross_partition = is_dynamic_bucket && !schema.partition_keys().is_empty() && {
            let pk_set: HashSet<&str> = schema.primary_keys().iter().map(String::as_str).collect();
            schema
                .partition_keys()
                .iter()
                .any(|p| !pk_set.contains(p.as_str()))
        };

        if has_primary_keys
            && !is_dynamic_bucket
            && total_buckets < 1
            && total_buckets != POSTPONE_BUCKET
        {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "KeyValueFileWriter does not support bucket={total_buckets}, only fixed bucket (>= 1), -1 (dynamic), or -2 (postpone) is supported"
                ),
            });
        }
        if has_primary_keys
            && total_buckets != POSTPONE_BUCKET
            && core_options
                .changelog_producer()
                .eq_ignore_ascii_case("input")
        {
            return Err(crate::Error::Unsupported {
                message: "KeyValueFileWriter does not support changelog-producer=input".to_string(),
            });
        }

        if !has_primary_keys && total_buckets != -1 && core_options.bucket_key().is_none() {
            return Err(crate::Error::Unsupported {
                message: "Append tables with fixed bucket must configure 'bucket-key'".to_string(),
            });
        }
        let target_file_size = core_options.target_file_size();
        let file_compression = core_options.file_compression().to_string();
        let file_compression_zstd_level = core_options.file_compression_zstd_level();
        let write_buffer_size = core_options.write_parquet_buffer_size();
        let partition_keys: Vec<String> = schema.partition_keys().to_vec();
        let fields = schema.fields();

        let partition_field_indices: Vec<usize> = partition_keys
            .iter()
            .filter_map(|pk| fields.iter().position(|f| f.name() == pk))
            .collect();

        // Bucket keys: resolved by TableSchema
        let bucket_keys = schema.bucket_keys();

        let bucket_key_indices: Vec<usize> = bucket_keys
            .iter()
            .filter_map(|bk| fields.iter().position(|f| f.name() == bk))
            .collect();

        let partition_computer = PartitionComputer::new(
            &partition_keys,
            fields,
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )
        .unwrap();

        let primary_key_indices: Vec<usize> = schema
            .trimmed_primary_keys()
            .iter()
            .filter_map(|pk| fields.iter().position(|f| f.name() == pk))
            .collect();

        let primary_key_types: Vec<DataType> = primary_key_indices
            .iter()
            .map(|&idx| fields[idx].data_type().clone())
            .collect();

        let sequence_field_indices: Vec<usize> = core_options
            .sequence_fields()
            .iter()
            .filter_map(|sf| fields.iter().position(|f| f.name() == *sf))
            .collect();

        let merge_engine = core_options.merge_engine()?;

        if has_primary_keys && core_options.rowkind_field().is_some() {
            return Err(crate::Error::Unsupported {
                message: "KeyValueFileWriter does not support rowkind.field".to_string(),
            });
        }

        let target_bucket_row_number = core_options.dynamic_bucket_target_row_num();

        let bucket_assigner = if is_cross_partition {
            BucketAssignerEnum::CrossPartition(Box::new(CrossPartitionAssigner::new(
                table.clone(),
                partition_field_indices,
                primary_key_indices.clone(),
                target_bucket_row_number,
                merge_engine,
            )))
        } else if is_dynamic_bucket {
            BucketAssignerEnum::Dynamic(DynamicBucketAssigner::new(
                partition_field_indices,
                primary_key_indices.clone(),
                schema.fields().to_vec(),
                target_bucket_row_number,
                table.file_io().clone(),
                table.location().to_string(),
                is_overwrite,
            ))
        } else if total_buckets == POSTPONE_BUCKET {
            BucketAssignerEnum::Constant(ConstantBucketAssigner::new(
                partition_field_indices,
                POSTPONE_BUCKET,
            ))
        } else if total_buckets <= 1 || bucket_key_indices.is_empty() {
            BucketAssignerEnum::Constant(ConstantBucketAssigner::new(partition_field_indices, 0))
        } else {
            BucketAssignerEnum::Fixed(FixedBucketAssigner::new(
                partition_field_indices,
                bucket_key_indices,
                total_buckets,
            ))
        };

        Ok(Self {
            table: table.clone(),
            partition_writers: HashMap::new(),
            partition_computer,
            partition_keys,
            schema_id: schema.id(),
            target_file_size,
            file_compression,
            file_compression_zstd_level,
            write_buffer_size,
            primary_key_indices,
            primary_key_types,
            sequence_field_indices,
            merge_engine,
            partition_seq_cache: HashMap::new(),
            commit_user,
            bucket_assigner,
            is_overwrite,
        })
    }

    /// Scan the latest snapshot for a specific partition and return a map of
    /// bucket → (max_sequence_number + 1) for each bucket in that partition.
    async fn scan_partition_sequence_numbers(
        table: &Table,
        partition_bytes: &[u8],
    ) -> crate::Result<HashMap<i32, i64>> {
        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        let latest_snapshot = snapshot_manager.get_latest_snapshot().await?;
        let mut bucket_seq: HashMap<i32, i64> = HashMap::new();
        if let Some(snapshot) = latest_snapshot {
            let partition_predicate = Self::build_partition_predicate(table, partition_bytes)?;
            let scan = TableScan::new(table, partition_predicate, vec![], None, None, None)
                .with_scan_all_files();
            let entries = scan.plan_manifest_entries(&snapshot).await?;
            for entry in &entries {
                let bucket = entry.bucket();
                let max_seq = entry.file().max_sequence_number;
                let current = bucket_seq.entry(bucket).or_insert(0);
                if max_seq + 1 > *current {
                    *current = max_seq + 1;
                }
            }
        }
        Ok(bucket_seq)
    }

    /// Build a partition predicate from serialized partition bytes.
    fn build_partition_predicate(
        table: &Table,
        partition_bytes: &[u8],
    ) -> crate::Result<Option<Predicate>> {
        let partition_fields = table.schema().partition_fields();
        if partition_fields.is_empty() {
            return Ok(None);
        }
        let partition_row = BinaryRow::from_serialized_bytes(partition_bytes)?;
        let fields: Vec<(&str, Option<Datum>)> = partition_fields
            .iter()
            .enumerate()
            .map(|(pos, field)| {
                let datum = partition_row.get_datum(pos, field.data_type())?;
                Ok((field.name(), datum))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let pred_builder = PredicateBuilder::new(table.schema().fields());
        Ok(Some(pred_builder.partition_predicate(&fields)?))
    }

    /// Write an Arrow RecordBatch. Rows are routed to the correct partition and bucket.
    pub async fn write_arrow_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let grouped = self.divide_by_partition_bucket(batch).await?;
        for ((partition_bytes, bucket), sub_batch) in grouped {
            self.write_bucket(partition_bytes, bucket, sub_batch)
                .await?;
        }
        Ok(())
    }

    /// Group rows by (partition_bytes, bucket) and return sub-batches.
    ///
    /// In cross-partition mode, also generates DELETE sub-batches for keys that
    /// migrated from one partition to another.
    async fn divide_by_partition_bucket(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<Vec<(PartitionBucketKey, RecordBatch)>> {
        // Fast path: constant bucket with no partitions — skip per-row routing
        if let BucketAssignerEnum::Constant(ref a) = self.bucket_assigner {
            if self.partition_keys.is_empty() {
                return Ok(vec![(
                    (EMPTY_SERIALIZED_ROW.clone(), a.bucket()),
                    batch.clone(),
                )]);
            }
        }

        let fields = self.table.schema().fields().to_vec();
        let output = self.bucket_assigner.assign_batch(batch, &fields).await?;

        let mut groups: HashMap<PartitionBucketKey, Vec<usize>> = HashMap::new();
        let skip_set: HashSet<usize> = output.skips.into_iter().collect();
        for row_idx in 0..batch.num_rows() {
            if skip_set.contains(&row_idx) {
                continue;
            }
            groups
                .entry((
                    output.partition_bytes[row_idx].clone(),
                    output.buckets[row_idx],
                ))
                .or_default()
                .push(row_idx);
        }

        let mut result = Vec::with_capacity(groups.len());
        // Cross-partition writers must always include _VALUE_KIND to keep the
        // Arrow schema stable across batches (some batches may have deletes,
        // others may not — KeyValueFileWriter's concat_batches requires a
        // consistent schema).
        let needs_value_kind =
            matches!(self.bucket_assigner, BucketAssignerEnum::CrossPartition(_))
                || !output.deletes.is_empty();
        for (key, row_indices) in groups {
            let sub_batch = Self::take_rows(batch, &row_indices)?;
            let sub_batch = if needs_value_kind {
                Self::add_value_kind_column(&sub_batch, 0)?
            } else {
                sub_batch
            };
            result.push((key, sub_batch));
        }

        if !output.deletes.is_empty() {
            let mut delete_groups: HashMap<PartitionBucketKey, Vec<usize>> = HashMap::new();
            for (row_idx, old_partition, old_bucket) in &output.deletes {
                delete_groups
                    .entry((old_partition.clone(), *old_bucket))
                    .or_default()
                    .push(*row_idx);
            }
            for (key, row_indices) in delete_groups {
                let sub_batch = Self::take_rows(batch, &row_indices)?;
                let delete_batch = Self::add_value_kind_column(&sub_batch, 1)?;
                result.push((key, delete_batch));
            }
        }

        Ok(result)
    }

    /// Extract rows from a batch by indices.
    fn take_rows(batch: &RecordBatch, row_indices: &[usize]) -> Result<RecordBatch> {
        if row_indices.len() == batch.num_rows() {
            return Ok(batch.clone());
        }
        let indices = arrow_array::UInt32Array::from(
            row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let columns: Vec<Arc<dyn arrow_array::Array>> = batch
            .columns()
            .iter()
            .map(|col| arrow_select::take::take(col.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to take rows: {e}"),
                source: None,
            })?;
        RecordBatch::try_new(batch.schema(), columns).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to create sub-batch: {e}"),
            source: None,
        })
    }

    /// Add a `_VALUE_KIND` column to a batch with the given value for all rows.
    fn add_value_kind_column(batch: &RecordBatch, value_kind: i8) -> Result<RecordBatch> {
        use arrow_array::Int8Array;
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};

        let vk_array = Arc::new(Int8Array::from(vec![value_kind; batch.num_rows()]));
        let vk_field = Arc::new(ArrowField::new(
            crate::spec::VALUE_KIND_FIELD_NAME,
            ArrowDataType::Int8,
            false,
        ));

        let mut fields = batch.schema().fields().to_vec();
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();
        fields.push(vk_field);
        columns.push(vk_array);

        let schema = Arc::new(arrow_schema::Schema::new(fields));
        RecordBatch::try_new(schema, columns).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to add _VALUE_KIND column: {e}"),
            source: None,
        })
    }

    /// Write a batch directly to the writer for the given (partition, bucket).
    async fn write_bucket(
        &mut self,
        partition_bytes: Vec<u8>,
        bucket: i32,
        batch: RecordBatch,
    ) -> Result<()> {
        let key = (partition_bytes, bucket);
        if !self.partition_writers.contains_key(&key) {
            self.create_writer(key.0.clone(), key.1).await?;
        }
        let writer = self.partition_writers.get_mut(&key).unwrap();
        writer.write(&batch).await
    }

    /// Write multiple Arrow RecordBatches.
    pub async fn write_arrow(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.write_arrow_batch(batch).await?;
        }
        Ok(())
    }

    /// Close all writers and collect CommitMessages for use with TableCommit.
    /// Writers are cleared after this call, allowing the TableWrite to be reused.
    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        let writers: Vec<(PartitionBucketKey, FileWriter)> =
            self.partition_writers.drain().collect();

        let futures: Vec<_> = writers
            .into_iter()
            .map(|((partition_bytes, bucket), writer)| async move {
                let files = writer.prepare_commit().await?;
                Ok::<_, crate::Error>((partition_bytes, bucket, files))
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;

        // Collect index files from bucket assigner
        let file_io = self.table.file_io();
        let index_dir = format!("{}/index", self.table.location());
        let mut index_files_by_key = self
            .bucket_assigner
            .prepare_commit_index(file_io, &index_dir)
            .await?;

        let mut messages = Vec::new();
        for (partition_bytes, bucket, files) in results {
            let key = (partition_bytes.clone(), bucket);
            let index_files = index_files_by_key.remove(&key).unwrap_or_default();
            if !files.is_empty() || !index_files.is_empty() {
                let mut msg = CommitMessage::new(partition_bytes, bucket, files);
                msg.new_index_files = index_files;
                messages.push(msg);
            }
        }
        // Emit index-only messages for (partition, bucket) pairs that had no data writer
        // (e.g., old buckets where keys migrated away in cross-partition mode).
        for ((partition_bytes, bucket), idx_files) in index_files_by_key {
            if !idx_files.is_empty() {
                let mut msg = CommitMessage::new(partition_bytes, bucket, vec![]);
                msg.new_index_files = idx_files;
                messages.push(msg);
            }
        }
        Ok(messages)
    }

    async fn create_writer(&mut self, partition_bytes: Vec<u8>, bucket: i32) -> Result<()> {
        let partition_path = self.resolve_partition_path(&partition_bytes)?;

        let writer = if self.primary_key_indices.is_empty() {
            self.create_append_writer(partition_path, bucket)
        } else if bucket == POSTPONE_BUCKET {
            self.create_postpone_writer(partition_path, bucket)
        } else {
            self.create_kv_writer(partition_path, bucket, &partition_bytes)
                .await?
        };

        self.partition_writers
            .insert((partition_bytes, bucket), writer);
        Ok(())
    }

    fn resolve_partition_path(&self, partition_bytes: &[u8]) -> Result<String> {
        if self.partition_keys.is_empty() {
            Ok(String::new())
        } else {
            let row = BinaryRow::from_serialized_bytes(partition_bytes)?;
            self.partition_computer.generate_partition_path(&row)
        }
    }

    /// Create an append-only writer for non-PK tables.
    fn create_append_writer(&self, partition_path: String, bucket: i32) -> FileWriter {
        FileWriter::Append(DataFileWriter::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
            partition_path,
            bucket,
            self.schema_id,
            self.target_file_size,
            self.file_compression.clone(),
            self.file_compression_zstd_level,
            self.write_buffer_size,
            Some(0), // file_source: APPEND
            None,    // first_row_id: assigned by commit
            None,    // write_cols: full-row write
        ))
    }

    /// Create a postpone writer (KV format, no sorting/dedup, special file naming).
    fn create_postpone_writer(&self, partition_path: String, bucket: i32) -> FileWriter {
        let data_file_prefix = format!("data-u-{}-s-0-w-", self.commit_user);
        FileWriter::Postpone(PostponeFileWriter::new(
            self.table.file_io().clone(),
            PostponeWriteConfig {
                table_location: self.table.location().to_string(),
                partition_path,
                bucket,
                schema_id: self.schema_id,
                target_file_size: self.target_file_size,
                file_compression: self.file_compression.clone(),
                file_compression_zstd_level: self.file_compression_zstd_level,
                write_buffer_size: self.write_buffer_size,
                data_file_prefix,
            },
        ))
    }

    /// Create a key-value writer for PK tables with normal buckets.
    async fn create_kv_writer(
        &mut self,
        partition_path: String,
        bucket: i32,
        partition_bytes: &[u8],
    ) -> Result<FileWriter> {
        // Lazily scan partition sequence numbers on first writer creation per partition.
        // Overwrite mode skips this — old data will be replaced, so seq starts at 0.
        if !self.is_overwrite && !self.partition_seq_cache.contains_key(partition_bytes) {
            let bucket_seq =
                Self::scan_partition_sequence_numbers(&self.table, partition_bytes).await?;
            self.partition_seq_cache
                .insert(partition_bytes.to_vec(), bucket_seq);
        }
        let next_seq = self
            .partition_seq_cache
            .get(partition_bytes)
            .and_then(|m| m.get(&bucket))
            .copied()
            .unwrap_or(0);

        Ok(FileWriter::KeyValue(KeyValueFileWriter::new(
            self.table.file_io().clone(),
            KeyValueWriteConfig {
                table_location: self.table.location().to_string(),
                partition_path,
                bucket,
                schema_id: self.schema_id,
                file_compression: self.file_compression.clone(),
                file_compression_zstd_level: self.file_compression_zstd_level,
                write_buffer_size: self.write_buffer_size,
                primary_key_indices: self.primary_key_indices.clone(),
                primary_key_types: self.primary_key_types.clone(),
                sequence_field_indices: self.sequence_field_indices.clone(),
                merge_engine: self.merge_engine,
            },
            next_seq,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        BinaryRowBuilder, BlobType, DataField, DataType, DecimalType, IntType,
        LocalZonedTimestampType, RowType, Schema, TableSchema, TimestampType, VarCharType,
    };
    use crate::table::{SnapshotManager, TableCommit};
    use arrow_array::Int32Array;
    use arrow_array::RecordBatchReader as _;
    use arrow_schema::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
    };
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_partitioned_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_schema(),
            None,
        )
    }

    fn test_partitioned_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_partitioned_schema(),
            None,
        )
    }

    fn test_blob_table_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_nested_blob_table_schema() -> TableSchema {
        let schema = Schema::builder()
            .column(
                "payload",
                DataType::Row(RowType::new(vec![DataField::new(
                    1,
                    "blob".into(),
                    DataType::Blob(BlobType::new()),
                )])),
            )
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn make_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_partitioned_batch(pts: Vec<&str>, ids: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("pt", ArrowDataType::Utf8, false),
            ArrowField::new("id", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(pts)),
                Arc::new(Int32Array::from(ids)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_and_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![1, 2, 3], vec![10, 20, 30]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 0);
        assert_eq!(messages[0].new_files.len(), 1);
        assert_eq!(messages[0].new_files[0].row_count, 3);

        // Commit and verify snapshot
        let commit = TableCommit::new(table, "test-user".to_string());
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(3));
    }

    #[test]
    fn test_rejects_blob_table() {
        let table = Table::new(
            test_file_io(),
            Identifier::new("default", "test_blob_table"),
            "memory:/test_blob_table".to_string(),
            test_blob_table_schema(),
            None,
        );

        let err = TableWrite::new(&table, "test-user".to_string(), false)
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("BlobType"))
        );
    }

    #[test]
    fn test_rejects_nested_blob_table() {
        let table = Table::new(
            test_file_io(),
            Identifier::new("default", "test_nested_blob_table"),
            "memory:/test_nested_blob_table".to_string(),
            test_nested_blob_table_schema(),
            None,
        );

        let err = TableWrite::new(&table, "test-user".to_string(), false)
            .err()
            .unwrap();
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("BlobType"))
        );
    }

    #[tokio::test]
    async fn test_write_partitioned() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_partitioned";
        setup_dirs(&file_io, table_path).await;

        let table = test_partitioned_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_partitioned_batch(vec!["a", "b", "a"], vec![1, 2, 3]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        // Should have 2 commit messages (one per partition)
        assert_eq!(messages.len(), 2);

        let total_rows: i64 = messages
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        assert_eq!(total_rows, 3);

        // Commit and verify
        let commit = TableCommit::new(table, "test-user".to_string());
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(3));
    }

    #[tokio::test]
    async fn test_write_empty_batch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_empty";
        let table = test_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![], vec![]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn test_prepare_commit_reusable() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_reuse";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // First write + prepare_commit
        table_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        let messages1 = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages1.len(), 1);
        assert_eq!(messages1[0].new_files[0].row_count, 2);

        // Second write + prepare_commit (reuse)
        table_write
            .write_arrow_batch(&make_batch(vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        let messages2 = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages2.len(), 1);
        assert_eq!(messages2[0].new_files[0].row_count, 3);

        // Empty prepare_commit is fine
        let messages3 = table_write.prepare_commit().await.unwrap();
        assert!(messages3.is_empty());
    }

    #[tokio::test]
    async fn test_write_multiple_batches() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_multi";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        table_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        table_write
            .write_arrow_batch(&make_batch(vec![3, 4], vec![30, 40]))
            .await
            .unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        // Multiple batches accumulate into a single file
        assert_eq!(messages[0].new_files.len(), 1);

        let total_rows: i64 = messages[0].new_files.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 4);
    }

    fn test_bucketed_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("bucket", "4")
            .option("bucket-key", "id")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_bucketed_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_bucketed_schema(),
            None,
        )
    }

    /// Build a batch where the bucket-key column ("id") is nullable.
    fn make_nullable_id_batch(ids: Vec<Option<i32>>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_bucketed_with_null_bucket_key() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_null_bk";
        setup_dirs(&file_io, table_path).await;

        let table = test_bucketed_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // Row with NULL bucket key should not panic
        let batch = make_nullable_id_batch(vec![None, Some(1), None], vec![10, 20, 30]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        let total_rows: i64 = messages
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_null_bucket_key_routes_consistently() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_null_bk_consistent";
        setup_dirs(&file_io, table_path).await;

        let table = test_bucketed_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // Two NULLs should land in the same bucket
        let batch = make_nullable_id_batch(vec![None, None], vec![10, 20]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        // Both NULL-key rows must be in the same (partition, bucket) group
        let null_bucket_rows: i64 = messages
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        assert_eq!(null_bucket_rows, 2);
        // All NULL-key rows go to exactly one bucket
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_null_vs_nonnull_bucket_key_differ() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_null_vs_nonnull";
        setup_dirs(&file_io, table_path).await;

        let table = test_bucketed_table(&file_io, table_path);

        // Compute bucket for NULL key
        let fields = table.schema().fields().to_vec();
        let mut tw = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch_null = make_nullable_id_batch(vec![None], vec![10]);
        let out_null = tw
            .bucket_assigner
            .assign_batch(&batch_null, &fields)
            .await
            .unwrap();
        let bucket_null = out_null.buckets[0];

        // Compute bucket for key = 0 (the value a null field's fixed bytes happen to be)
        let batch_zero = make_nullable_id_batch(vec![Some(0)], vec![20]);
        let out_zero = tw
            .bucket_assigner
            .assign_batch(&batch_zero, &fields)
            .await
            .unwrap();
        let bucket_zero = out_zero.buckets[0];

        // A NULL bucket key must produce a BinaryRow with the null bit set,
        // which hashes differently from a non-null 0 value.
        // (With 4 buckets they could theoretically collide, but the hash codes differ.)
        let mut builder_null = BinaryRowBuilder::new(1);
        builder_null.set_null_at(0);
        let hash_null = builder_null.build().hash_code();

        let mut builder_zero = BinaryRowBuilder::new(1);
        builder_zero.write_int(0, 0);
        let hash_zero = builder_zero.build().hash_code();

        assert_ne!(hash_null, hash_zero, "NULL and 0 should hash differently");
        // If hashes differ, buckets should differ (with 4 buckets, very likely)
        // But we verify the hash difference is the important invariant
        let _ = (bucket_null, bucket_zero);
    }

    /// Mirrors Java's testUnCompactDecimalAndTimestampNullValueBucketNumber.
    /// Non-compact types (Decimal(38,18), LocalZonedTimestamp(6), Timestamp(6))
    /// use variable-length encoding in BinaryRow — NULL handling must still work.
    #[tokio::test]
    async fn test_non_compact_null_bucket_key() {
        let file_io = test_file_io();

        let bucket_cols = ["d", "ltz", "ntz"];
        let total_buckets = 16;

        for bucket_col in &bucket_cols {
            let table_path = format!("memory:/test_null_bk_{bucket_col}");
            setup_dirs(&file_io, &table_path).await;

            let schema = Schema::builder()
                .column("d", DataType::Decimal(DecimalType::new(38, 18).unwrap()))
                .column(
                    "ltz",
                    DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                )
                .column("ntz", DataType::Timestamp(TimestampType::new(6).unwrap()))
                .column("k", DataType::Int(IntType::new()))
                .option("bucket", total_buckets.to_string())
                .option("bucket-key", *bucket_col)
                .build()
                .unwrap();
            let table_schema = TableSchema::new(0, &schema);
            let table = Table::new(
                file_io.clone(),
                Identifier::new("default", "test_table"),
                table_path.to_string(),
                table_schema,
                None,
            );

            let mut tw = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
            let fields = table.schema().fields().to_vec();

            // Build a batch: d=NULL, ltz=NULL, ntz=NULL, k=1
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("d", ArrowDataType::Decimal128(38, 18), true),
                ArrowField::new(
                    "ltz",
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                ArrowField::new(
                    "ntz",
                    ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                ArrowField::new("k", ArrowDataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(
                        arrow_array::Decimal128Array::from(vec![None::<i128>])
                            .with_precision_and_scale(38, 18)
                            .unwrap(),
                    ),
                    Arc::new(
                        arrow_array::TimestampMicrosecondArray::from(vec![None::<i64>])
                            .with_timezone("UTC"),
                    ),
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                        None::<i64>,
                    ])),
                    Arc::new(Int32Array::from(vec![1])),
                ],
            )
            .unwrap();

            let batch_output = tw
                .bucket_assigner
                .assign_batch(&batch, &fields)
                .await
                .unwrap();
            let bucket = batch_output.buckets[0];

            // Expected: BinaryRow with 1 field, null at pos 0
            let mut builder = BinaryRowBuilder::new(1);
            builder.set_null_at(0);
            let expected_bucket = (builder.build().hash_code() % total_buckets).wrapping_abs();

            assert_eq!(
                bucket, expected_bucket,
                "NULL bucket-key '{bucket_col}' should produce bucket {expected_bucket}, got {bucket}"
            );
        }
    }

    #[tokio::test]
    async fn test_write_rolling_on_target_file_size() {
        let file_io = test_file_io();
        let table_path = "memory:/test_table_write_rolling";
        setup_dirs(&file_io, table_path).await;

        // Create table with very small target-file-size to trigger rolling
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("target-file-size", "1b")
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // Write multiple batches — each should roll to a new file
        table_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        table_write
            .write_arrow_batch(&make_batch(vec![3, 4], vec![30, 40]))
            .await
            .unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        // With 1-byte target, each batch should produce a separate file
        assert_eq!(messages[0].new_files.len(), 2);

        let total_rows: i64 = messages[0].new_files.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 4);
    }

    // -----------------------------------------------------------------------
    // Primary-key table write tests
    // -----------------------------------------------------------------------

    fn test_pk_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_pk_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_pk_table"),
            table_path.to_string(),
            test_pk_schema(),
            None,
        )
    }

    #[tokio::test]
    async fn test_pk_write_and_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_pk_write";
        setup_dirs(&file_io, table_path).await;

        let table = test_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![3, 1, 2], vec![30, 10, 20]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);

        let file = &messages[0].new_files[0];
        assert_eq!(file.row_count, 3);
        assert_eq!(file.level, 0);
        assert_eq!(file.min_sequence_number, 0);
        assert_eq!(file.max_sequence_number, 2);
        // min_key and max_key should be non-empty (serialized BinaryRow)
        assert!(!file.min_key.is_empty());
        assert!(!file.max_key.is_empty());

        // Commit
        let commit = TableCommit::new(table.clone(), "test-user".to_string());
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(3));
    }

    #[tokio::test]
    async fn test_pk_write_sorted_output() {
        let file_io = test_file_io();
        let table_path = "memory:/test_pk_sorted";
        setup_dirs(&file_io, table_path).await;

        let table = test_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // Write unsorted data
        let batch = make_batch(vec![5, 2, 4, 1, 3], vec![50, 20, 40, 10, 30]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        let commit = TableCommit::new(table.clone(), "test-user".to_string());
        commit.commit(messages).await.unwrap();

        // Read back using sort-merge reader — should be sorted by PK
        let rb = table.new_read_builder();
        let scan = rb.new_scan();
        let plan = scan.plan().await.unwrap();
        let read = rb.new_read().unwrap();
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
                .await
                .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);

        let values: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![10, 20, 30, 40, 50]);
    }

    #[tokio::test]
    async fn test_pk_write_dedup_across_commits() {
        let file_io = test_file_io();
        let table_path = "memory:/test_pk_dedup";
        setup_dirs(&file_io, table_path).await;

        let table = test_pk_table(&file_io, table_path);

        // First commit: id=1,2,3
        let mut tw1 = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
        tw1.write_arrow_batch(&make_batch(vec![1, 2, 3], vec![10, 20, 30]))
            .await
            .unwrap();
        let msgs1 = tw1.prepare_commit().await.unwrap();
        let commit = TableCommit::new(table.clone(), "test-user".to_string());
        commit.commit(msgs1).await.unwrap();

        // Second commit: id=2,3,4 with updated values, higher sequence numbers
        let mut tw2 = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
        tw2.write_arrow_batch(&make_batch(vec![2, 3, 4], vec![200, 300, 400]))
            .await
            .unwrap();
        let msgs2 = tw2.prepare_commit().await.unwrap();
        commit.commit(msgs2).await.unwrap();

        // Read back — dedup should keep newer values for id=2,3
        let rb = table.new_read_builder();
        let scan = rb.new_scan();
        let plan = scan.plan().await.unwrap();
        let read = rb.new_read().unwrap();
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
                .await
                .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(ids, vec![1, 2, 3, 4]);
        assert_eq!(values, vec![10, 200, 300, 400]);
    }

    #[tokio::test]
    async fn test_pk_write_sequence_number_in_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_pk_seq";
        setup_dirs(&file_io, table_path).await;

        let table = test_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![1, 2], vec![10, 20]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];
        // Fresh table, seq starts at 0, 2 rows → min=0, max=1
        assert_eq!(file.min_sequence_number, 0);
        assert_eq!(file.max_sequence_number, 1);
    }

    // -----------------------------------------------------------------------
    // Postpone bucket (bucket = -2) write tests
    // -----------------------------------------------------------------------

    fn test_postpone_pk_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "-2")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_postpone_pk_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_postpone_table"),
            table_path.to_string(),
            test_postpone_pk_schema(),
            None,
        )
    }

    fn test_postpone_partitioned_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .option("bucket", "-2")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_postpone_partitioned_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_postpone_table"),
            table_path.to_string(),
            test_postpone_partitioned_schema(),
            None,
        )
    }

    fn make_partitioned_batch_3col(pts: Vec<&str>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("pt", ArrowDataType::Utf8, false),
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(pts)),
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_postpone_write_and_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_write";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![3, 1, 2], vec![30, 10, 20]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, POSTPONE_BUCKET);
        assert_eq!(messages[0].new_files.len(), 1);
        assert_eq!(messages[0].new_files[0].row_count, 3);

        // Commit and verify snapshot
        let commit = TableCommit::new(table, "test-user".to_string());
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(3));
    }

    #[tokio::test]
    async fn test_postpone_write_empty_batch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_empty";
        let table = test_postpone_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch = make_batch(vec![], vec![]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn test_postpone_write_multiple_batches() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_multi";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        table_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        table_write
            .write_arrow_batch(&make_batch(vec![3, 4], vec![30, 40]))
            .await
            .unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, POSTPONE_BUCKET);

        let total_rows: i64 = messages[0].new_files.iter().map(|f| f.row_count).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_postpone_write_partitioned() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_partitioned";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_partitioned_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        let batch =
            make_partitioned_batch_3col(vec!["a", "b", "a"], vec![1, 2, 3], vec![10, 20, 30]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        // 2 partitions
        assert_eq!(messages.len(), 2);
        // All messages should use POSTPONE_BUCKET
        for msg in &messages {
            assert_eq!(msg.bucket, POSTPONE_BUCKET);
        }

        let total_rows: i64 = messages
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        assert_eq!(total_rows, 3);

        // Commit and verify
        let commit = TableCommit::new(table, "test-user".to_string());
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(3));
    }

    #[tokio::test]
    async fn test_postpone_write_reusable() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_reuse";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // First write + prepare_commit
        table_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        let messages1 = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages1.len(), 1);
        assert_eq!(messages1[0].new_files[0].row_count, 2);

        // Second write + prepare_commit (reuse)
        table_write
            .write_arrow_batch(&make_batch(vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        let messages2 = table_write.prepare_commit().await.unwrap();
        assert_eq!(messages2.len(), 1);
        assert_eq!(messages2[0].new_files[0].row_count, 3);

        // Empty prepare_commit
        let messages3 = table_write.prepare_commit().await.unwrap();
        assert!(messages3.is_empty());
    }

    #[tokio::test]
    async fn test_postpone_write_file_naming_and_kv_format() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_kv";
        setup_dirs(&file_io, table_path).await;

        let table = test_postpone_pk_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "my-commit-user".to_string(), false).unwrap();

        let batch = make_batch(vec![3, 1, 2], vec![30, 10, 20]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];

        // Verify postpone file naming: data-u-{commitUser}-s-{writeId}-w-{uuid}-{index}.parquet
        assert!(
            file.file_name.starts_with("data-u-my-commit-user-s-"),
            "Expected postpone file prefix, got: {}",
            file.file_name
        );
        assert!(
            file.file_name.contains("-w-"),
            "Expected -w- in file name, got: {}",
            file.file_name
        );

        // Verify KV format: read the parquet file and check physical columns
        let bucket_dir = format!("{table_path}/bucket-postpone");
        let file_path = format!("{bucket_dir}/{}", file.file_name);
        let input = file_io.new_input(&file_path).unwrap();
        let data = input.read().await.unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(data, 1024).unwrap();
        let schema = reader.schema();
        // Physical schema: [_SEQUENCE_NUMBER, _VALUE_KIND, id, value]
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "_SEQUENCE_NUMBER");
        assert_eq!(schema.field(1).name(), "_VALUE_KIND");
        assert_eq!(schema.field(2).name(), "id");
        assert_eq!(schema.field(3).name(), "value");

        // Data should be in arrival order (not sorted by PK): 3, 1, 2
        let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(
            ids,
            vec![3, 1, 2],
            "Postpone mode should preserve arrival order"
        );

        // Empty key stats for postpone mode
        assert_eq!(file.min_key, EMPTY_SERIALIZED_ROW.clone());
        assert_eq!(file.max_key, EMPTY_SERIALIZED_ROW.clone());
    }

    // -----------------------------------------------------------------------
    // Cross-partition update tests (dynamic bucket, PK not including partition)
    // -----------------------------------------------------------------------

    fn test_cross_partition_schema() -> TableSchema {
        // PK is only "id", partition is "pt" — PK does NOT include partition field
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .partition_keys(["pt"])
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_cross_partition_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_cross_partition"),
            table_path.to_string(),
            test_cross_partition_schema(),
            None,
        )
    }

    #[tokio::test]
    async fn test_cross_partition_detection() {
        let file_io = test_file_io();
        let table_path = "memory:/test_cross_detect";

        // Cross-partition: PK=id, partition=pt, bucket=-1
        let table = test_cross_partition_table(&file_io, table_path);
        let tw = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
        assert!(matches!(
            tw.bucket_assigner,
            BucketAssignerEnum::CrossPartition(_)
        ));

        // Non-cross-partition: PK includes partition field
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .build()
            .unwrap();
        let table2 = Table::new(
            file_io.clone(),
            Identifier::new("default", "test"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let tw2 = TableWrite::new(&table2, "test-user".to_string(), false).unwrap();
        assert!(matches!(
            tw2.bucket_assigner,
            BucketAssignerEnum::Dynamic(_)
        ));
    }

    #[tokio::test]
    async fn test_cross_partition_write_same_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_cross_same_pt";
        setup_dirs(&file_io, table_path).await;

        let table = test_cross_partition_table(&file_io, table_path);
        let mut table_write = TableWrite::new(&table, "test-user".to_string(), false).unwrap();

        // All rows in same partition — no cross-partition deletes
        let batch =
            make_partitioned_batch_3col(vec!["a", "a", "a"], vec![1, 2, 3], vec![10, 20, 30]);
        table_write.write_arrow_batch(&batch).await.unwrap();

        let messages = table_write.prepare_commit().await.unwrap();
        let total_data_files: usize = messages.iter().map(|m| m.new_files.len()).sum();
        assert!(total_data_files >= 1);

        let total_rows: i64 = messages
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_cross_partition_write_generates_delete() {
        let file_io = test_file_io();
        let table_path = "memory:/test_cross_delete";
        setup_dirs(&file_io, table_path).await;

        let table = test_cross_partition_table(&file_io, table_path);

        // First commit: id=1 in partition "a"
        let mut tw1 = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
        let batch1 = make_partitioned_batch_3col(vec!["a"], vec![1], vec![10]);
        tw1.write_arrow_batch(&batch1).await.unwrap();
        let msgs1 = tw1.prepare_commit().await.unwrap();
        let commit = TableCommit::new(table.clone(), "test-user".to_string());
        commit.commit(msgs1).await.unwrap();

        // Second commit: id=1 moves to partition "b"
        let mut tw2 = TableWrite::new(&table, "test-user".to_string(), false).unwrap();
        let batch2 = make_partitioned_batch_3col(vec!["b"], vec![1], vec![20]);
        tw2.write_arrow_batch(&batch2).await.unwrap();
        let msgs2 = tw2.prepare_commit().await.unwrap();

        // Should have messages for both partition "a" (delete) and partition "b" (add)
        assert!(
            msgs2.len() >= 2,
            "Expected messages for both old and new partition, got {}",
            msgs2.len()
        );

        // The total data files should include both the DELETE and ADD records
        let total_rows: i64 = msgs2
            .iter()
            .flat_map(|m| &m.new_files)
            .map(|f| f.row_count)
            .sum();
        // 1 ADD in partition "b" + 1 DELETE in partition "a"
        assert_eq!(total_rows, 2);

        commit.commit(msgs2).await.unwrap();

        // Read back — should only see id=1 with value=20 in partition "b"
        let rb = table.new_read_builder();
        let scan = rb.new_scan();
        let plan = scan.plan().await.unwrap();
        let read = rb.new_read().unwrap();
        let batches: Vec<RecordBatch> =
            futures::TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
                .await
                .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let values: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(ids, vec![1]);
        assert_eq!(values, vec![20]);
    }
}
