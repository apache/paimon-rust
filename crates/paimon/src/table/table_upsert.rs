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

//! Table-level append-only upsert by user-specified keys.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat_batches, take::take};
use futures::TryStreamExt;
use indexmap::IndexMap;

use super::global_index_freshness::indexed_columns_unchanged;
use super::key_normalization::canonical_partition_key;
use super::update_input::unique_column_index;
use super::upsert_key_matcher::UpsertKeyMatcher;
use super::write_batch_normalize::{normalize_blob_write_field, normalize_write_array};
use crate::spec::{
    batch_to_serialized_bytes, extract_datum_from_arrow, BinaryRow, CoreOptions, Predicate,
    PredicateBuilder,
};
use crate::table::source::{DataSplit, Plan};
use crate::table::{CommitMessage, Table};

const ROW_ID: &str = "_ROW_ID";

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn selected_rows(batch: &RecordBatch, rows: &[usize]) -> crate::Result<RecordBatch> {
    let indices = rows
        .iter()
        .map(|index| u32::try_from(*index).map_err(|_| invalid("upsert row index exceeds u32")))
        .collect::<crate::Result<Vec<_>>>()?;
    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            take(array.as_ref(), &indices, None)
                .map_err(|error| invalid(format!("cannot select upsert rows: {error}")))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| invalid(format!("cannot build selected upsert rows: {error}")))
}

/// Upsert Arrow rows into a data-evolution table without primary keys.
/// Internal executor for `TableUpdate::upsert_by_arrow_with_key`. Existing
/// keys are updated by row ID; new keys are appended.
#[must_use = "upsert must be used to call prepare_commit()"]
pub(super) struct TableUpsert {
    table: Table,
    commit_user: String,
    keys: Vec<String>,
    update_columns: Vec<String>,
    source: Vec<RecordBatch>,
    blob_uri_reader_factory: Option<Arc<dyn crate::io::UriReaderFactory>>,
}

impl TableUpsert {
    pub(super) fn new(
        table: &Table,
        commit_user: String,
        mut keys: Vec<String>,
        update_columns: Vec<String>,
    ) -> crate::Result<Self> {
        if keys.is_empty() {
            return Err(invalid("upsert keys must not be empty"));
        }
        // PyPaimon matches independently within each source partition, even
        // when callers omit partition columns from their upsert keys.
        for partition in table.schema().partition_keys() {
            if !keys.contains(partition) {
                keys.push(partition.clone());
            }
        }
        let fields = table.schema().fields();
        for key in &keys {
            if !fields.iter().any(|field| field.name() == key) {
                return Err(invalid(format!(
                    "upsert key '{key}' is not in table schema"
                )));
            }
        }
        let arrow_schema = crate::arrow::build_target_arrow_schema(fields)?;
        for key in &keys {
            let field = arrow_schema
                .field_with_name(key)
                .map_err(|error| invalid(format!("missing upsert key '{key}': {error}")))?;
            if !super::upsert_key_matcher::supported_key_type(field.data_type()) {
                return Err(crate::Error::Unsupported {
                    message: format!("unsupported upsert key type: {:?}", field.data_type()),
                });
            }
        }
        if update_columns.is_empty() {
            return Err(invalid("upsert update columns must not be empty"));
        }
        // Reuse the row-ID writer's precondition and column-path checks.
        let _validated_update =
            super::DataEvolutionWriter::for_row_id(table, update_columns.clone())?;
        Ok(Self {
            table: table.clone(),
            commit_user,
            keys,
            update_columns,
            source: Vec::new(),
            blob_uri_reader_factory: None,
        })
    }

    pub(super) fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<Arc<dyn crate::io::UriReaderFactory>>,
    ) {
        self.blob_uri_reader_factory = factory;
    }

    /// Add rows containing the keys and columns to write. Column order may
    /// differ from the table schema. Keep original payloads, including keys,
    /// until last-write-wins and matching have selected their action.
    /// Batches may carry different non-key columns, preserving absent fields.
    pub(super) fn add_batch(&mut self, batch: RecordBatch) -> crate::Result<()> {
        let target = crate::arrow::build_target_arrow_schema(self.table.schema().fields())?;
        let input_schema = batch.schema();
        for field in input_schema.fields() {
            unique_column_index(&input_schema, field.name())?;
            target.field_with_name(field.name()).map_err(|_| {
                invalid(format!(
                    "upsert column '{}' is not in table schema",
                    field.name()
                ))
            })?;
        }
        for key in &self.keys {
            unique_column_index(&input_schema, key)?;
        }
        let mut columns = Vec::with_capacity(target.fields().len());
        let mut fields = Vec::new();
        for field in target.fields() {
            let Some(column) = batch.column_by_name(field.name()) else {
                continue;
            };
            columns.push(column.clone());
            fields.push(input_schema.field_with_name(field.name()).unwrap().clone());
        }
        let ordered = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| invalid(format!("cannot order upsert columns: {error}")))?;
        self.source.push(ordered);
        Ok(())
    }

    /// Appends use the ordinary write contract; matched values use the row-ID
    /// updater's conversion rules. Neither action touches discarded source rows.
    fn normalize_selected_batch(
        &self,
        batch: RecordBatch,
        append: bool,
    ) -> crate::Result<RecordBatch> {
        let target = crate::arrow::build_target_arrow_schema(self.table.schema().fields())?;
        let update_fields = super::data_evolution_fields::project_by_paths(
            self.table.schema().fields(),
            &self.update_columns,
        )?;
        let inline = CoreOptions::new(self.table.schema().options()).blob_inline_fields();
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, field) in target.fields().iter().enumerate() {
            let Some(column) = batch.column_by_name(field.name()) else {
                // The row-ID updater diagnoses missing update columns. Absent
                // append columns stay absent in the file's write_cols metadata.
                continue;
            };
            let logical = &self.table.schema().fields()[index];
            let update_field = update_fields
                .iter()
                .find(|updated| updated.name() == field.name());
            let normalized = if !append && update_field.is_none() {
                Ok((
                    batch
                        .schema()
                        .field_with_name(field.name())
                        .unwrap()
                        .clone(),
                    column.clone(),
                ))
            } else if logical.data_type().is_blob_file_field()
                && !inline.contains(logical.name())
                && (append || matches!(logical.data_type(), crate::spec::DataType::Map(_)))
            {
                // Named-row Blob maps carry nullable keys and decimal text in
                // their dedicated physical layout. Keep that transport intact.
                if append {
                    normalize_blob_write_field(column, field)
                } else {
                    super::write_batch_normalize::normalize_blob_update_field(column, field)
                }
            } else if append {
                normalize_write_array(column, field.data_type())
                    .map(|column| (field.as_ref().clone(), column))
            } else {
                Ok((
                    batch
                        .schema()
                        .field_with_name(field.name())
                        .unwrap()
                        .clone(),
                    column.clone(),
                ))
            };
            let (field, column) = normalized.map_err(|error| {
                invalid(format!("Invalid upsert column '{}': {error}", field.name()))
            })?;
            fields.push(field);
            columns.push(column);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| invalid(format!("cannot normalize selected upsert rows: {error}")))
    }

    /// Concatenate only match keys. Padding payload columns with NULL would
    /// lose the distinction between an absent field and an explicit NULL.
    fn key_rows(&self) -> crate::Result<RecordBatch> {
        let target = crate::arrow::build_target_arrow_schema(self.table.schema().fields())?;
        let fields = target
            .fields()
            .iter()
            .filter(|field| self.keys.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>();
        let key_schema = Arc::new(Schema::new(fields));
        let batches = self
            .source
            .iter()
            .map(|batch| {
                let columns = key_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        super::key_normalization::normalize_match_key(
                            batch.column_by_name(field.name()).unwrap(),
                            field.data_type(),
                        )
                        .map_err(|error| {
                            invalid(format!("Invalid upsert key '{}': {error}", field.name()))
                        })
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                RecordBatch::try_new(key_schema.clone(), columns)
                    .map_err(|error| invalid(error.to_string()))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let first = batches
            .first()
            .ok_or_else(|| invalid("Input data is empty"))?;
        concat_batches(&first.schema(), &batches)
            .map_err(|error| invalid(format!("cannot concatenate upsert keys: {error}")))
    }

    /// Matcher indices are sorted by their original input position, with one
    /// repeated index per matching target row. The range retains alignment
    /// with the matcher's parallel row-ID vector across source batch boundaries.
    fn selected_batches(
        &self,
        indices: &[usize],
    ) -> crate::Result<Vec<(RecordBatch, Range<usize>)>> {
        let mut groups = Vec::new();
        let (mut offset, mut selected) = (0, 0);
        for source in &self.source {
            let end = offset + source.num_rows();
            let count = indices[selected..].partition_point(|index| *index < end);
            if count > 0 {
                let range = selected..selected + count;
                let rows = indices[range.clone()]
                    .iter()
                    .map(|index| index - offset)
                    .collect::<Vec<_>>();
                groups.push((selected_rows(source, &rows)?, range));
                selected += count;
            }
            offset = end;
        }
        Ok(groups)
    }

    fn serialized_partitions(&self, batch: &RecordBatch) -> crate::Result<Vec<Vec<u8>>> {
        let schema = self.table.schema();
        let indices = schema
            .partition_keys()
            .iter()
            .map(|name| unique_column_index(&batch.schema(), name))
            .collect::<crate::Result<Vec<_>>>()?;
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                schema
                    .fields()
                    .iter()
                    .find(|table_field| table_field.name() == field.name())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        batch_to_serialized_bytes(batch, &indices, &fields)
    }

    /// Java append writers use one write type for a partition's input. Like
    /// PyPaimon named-row upserts, validate only surviving unmatched rows;
    /// different partitions may append different field sets.
    fn append_groups(&self, indices: &[usize]) -> crate::Result<Vec<Vec<RecordBatch>>> {
        let mut groups: IndexMap<Vec<u8>, Vec<RecordBatch>> = IndexMap::new();
        for (batch, _) in self.selected_batches(indices)? {
            let batch = self.normalize_selected_batch(batch, true)?;
            let mut by_partition: IndexMap<Vec<u8>, Vec<usize>> = IndexMap::new();
            for (row, partition) in self.serialized_partitions(&batch)?.into_iter().enumerate() {
                by_partition.entry(partition).or_default().push(row);
            }
            for (partition, rows) in by_partition {
                let group = groups.entry(partition).or_default();
                if group
                    .first()
                    .is_some_and(|first| first.schema() != batch.schema())
                {
                    return Err(invalid("upsert_by_key requires appended rows in the same partition to have the same field set"));
                }
                group.push(selected_rows(&batch, &rows)?);
            }
        }
        Ok(groups.into_values().collect())
    }

    /// Match only the source partitions, as PyPaimon does. Filter planned splits
    /// after global row IDs have been assigned so pruning cannot renumber rows.
    fn matching_splits<'a>(
        &self,
        source: &RecordBatch,
        plan: &'a Plan,
    ) -> crate::Result<Cow<'a, [DataSplit]>> {
        let schema = self.table.schema();
        if schema.partition_keys().is_empty() {
            return Ok(Cow::Borrowed(plan.splits()));
        }
        let partitions: HashSet<_> = self.serialized_partitions(source)?.into_iter().collect();
        let fields = schema.partition_fields();
        let mut splits = Vec::new();
        for split in plan.splits() {
            if partitions.contains(&canonical_partition_key(split.partition(), &fields)?) {
                splits.push(split.clone());
            }
        }
        Ok(Cow::Owned(splits))
    }

    /// Each column domain is a superset of the input tuples. The matcher still
    /// checks complete keys, including NULLs, after index and reader pruning.
    fn lookup_filter(&self, source: &RecordBatch) -> crate::Result<Option<Predicate>> {
        let builder = PredicateBuilder::new(self.table.schema().fields());
        let mut predicates = Vec::new();
        for key in &self.keys {
            // Partition sets compare raw BinaryRows, which may use different
            // variable-length storage for Java NULL timestamps and decimals.
            // matching_splits checks their canonical logical identity instead.
            if self.table.schema().partition_keys().contains(key) {
                continue;
            }
            let column = unique_column_index(&source.schema(), key)?;
            // Arrow row encoding preserves NaN payloads and signed zero. SQL
            // equality need not share that identity, so retain the full domain.
            if matches!(
                source.column(column).data_type(),
                DataType::Float32 | DataType::Float64
            ) {
                continue;
            }
            let field = self
                .table
                .schema()
                .fields()
                .iter()
                .find(|field| field.name() == key)
                .unwrap();
            // BLOB keys can be matched as Arrow bytes but have no scalar Datum.
            if field.data_type().is_blob_type() {
                continue;
            }
            let mut seen = HashSet::new();
            let mut values = Vec::new();
            let mut has_null = false;
            for row in 0..source.num_rows() {
                match extract_datum_from_arrow(source, row, column, field.data_type())? {
                    Some(value) => {
                        if seen.insert(crate::btree::serialize_datum(&value, field.data_type())) {
                            values.push(value);
                        }
                    }
                    None => has_null = true,
                }
            }
            let mut alternatives = Vec::new();
            if values.len() == 1 {
                alternatives.push(builder.equal(key.as_str(), values.remove(0))?);
            } else if !values.is_empty() {
                alternatives.push(builder.is_in(key.as_str(), values)?);
            }
            if has_null {
                alternatives.push(builder.is_null(key.as_str())?);
            }
            if !alternatives.is_empty() {
                predicates.push(Predicate::or(alternatives));
            }
        }
        Ok((!predicates.is_empty()).then(|| Predicate::and(predicates)))
    }

    async fn match_existing(
        &self,
        source: &RecordBatch,
        matcher: &mut UpsertKeyMatcher,
    ) -> crate::Result<Plan> {
        // Mutation lookups must include unindexed rows even when queries use
        // fast mode, or a matching target would incorrectly become an insert.
        let lookup_options = HashMap::from([("scalar-index.search-mode".into(), "full".into())]);
        let mut lookup_table = self.table.copy_with_options(lookup_options);
        if lookup_table.schema().core_options().global_index_enabled() {
            let snapshot = super::time_travel::resolve_snapshot(&lookup_table).await?;
            lookup_table = lookup_table.copy_with_pinned_snapshot(snapshot.as_ref());
            if let Some(snapshot) = snapshot {
                let partitions = self.serialized_partitions(source)?.into_iter().collect();
                if !indexed_columns_unchanged(&lookup_table, &snapshot, &self.keys, &partitions)
                    .await
                    .unwrap_or(false)
                {
                    lookup_table = lookup_table.copy_with_options(HashMap::from([(
                        "global-index.enabled".into(),
                        "false".into(),
                    )]));
                }
            }
        }
        let mut scan_builder = lookup_table.new_read_builder();
        if let Some(filter) = self.lookup_filter(source)? {
            scan_builder.with_filter(filter);
        }
        let plan = scan_builder.new_scan().plan_all().await?;
        // Split row ranges retain index pruning. Exact tuple matching below
        // avoids evaluating a potentially large IN list again for every row.
        let mut read_builder = lookup_table.new_read_builder();
        let mut projection = self.keys.iter().map(String::as_str).collect::<Vec<_>>();
        projection.push(ROW_ID);
        read_builder.with_projection(&projection)?;
        let read = read_builder.new_read()?.for_update();
        let splits = self.matching_splits(source, &plan)?;
        let mut stream = read.to_arrow(splits.as_ref())?;
        while let Some(batch) = stream.try_next().await? {
            matcher.add_existing_batch(&batch)?;
        }
        Ok(plan)
    }

    /// Match source keys against the target snapshot, stage row-ID updates and
    /// new rows, then return both sets of commit messages as one operation.
    #[must_use = "commit messages must be passed to TableCommit"]
    pub(super) async fn prepare_commit(self) -> crate::Result<Vec<CommitMessage>> {
        let source = self.key_rows()?;
        if source.num_rows() == 0 {
            return Err(invalid("Input data is empty"));
        }
        let mut matcher = UpsertKeyMatcher::new(&source, self.keys.clone())?;

        let plan = self.match_existing(&source, &mut matcher).await?;
        let (matched_indices, row_ids, new_indices) = matcher.finish();

        // Validate append field sets before staging any updates or new files.
        let append_groups = self.append_groups(&new_indices)?;
        let matched_batches = self.selected_batches(&matched_indices)?;

        let mut messages = Vec::new();
        let result: crate::Result<()> = async {
            if !matched_indices.is_empty() {
                let mut update = super::DataEvolutionWriter::for_row_id(
                    &self.table,
                    self.update_columns.clone(),
                )?;
                update.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
                if let Some(snapshot_id) = plan.snapshot_id() {
                    update.pin_read_snapshot(snapshot_id);
                }
                for (selected, range) in matched_batches {
                    let selected = self.normalize_selected_batch(selected, false)?;
                    let mut fields = selected.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new(ROW_ID, DataType::Int64, false)));
                    let mut columns: Vec<ArrayRef> = selected.columns().to_vec();
                    columns.push(Arc::new(Int64Array::from(row_ids[range].to_vec())));
                    let matched = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                        .map_err(|error| {
                            invalid(format!("cannot build matched upsert rows: {error}"))
                        })?;
                    update.add_matched_batch(matched)?;
                }
                messages.extend(update.prepare_commit().await?);
            }
            for group in append_groups {
                let mut append = self
                    .table
                    .new_write_builder()
                    .with_commit_user(self.commit_user.clone())?
                    .new_write()?;
                append.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone())?;
                append.with_write_type(
                    group[0]
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().to_string())
                        .collect(),
                )?;
                for batch in group {
                    if let Err(error) = append.write_arrow_batch(&batch).await {
                        append.close().await;
                        return Err(error);
                    }
                }
                messages.extend(append.prepare_commit().await?);
            }
            Ok(())
        }
        .await;
        // Preserve earlier prepared groups on failure. Never delete files
        // based on CommitMessage; a failed response does not prove ownership.
        result.map(|()| messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataEvolutionIndexSourceMeta, DataType as PaimonType, IndexManifest, TimestampType,
        VarCharType,
    };
    use crate::spec::{IntType, Schema as PaimonSchema, TableSchema};
    use crate::table::TableCommit;
    use arrow_array::{Array, Int32Array, StringArray};

    async fn numeric_table(path: &str) -> Table {
        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .column("keep", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert"),
            path.into(),
            TableSchema::new(0, &schema),
            None,
        );
        let initial = RecordBatch::try_from_iter([
            ("key", Arc::new(Int32Array::from(vec![1, 1, 2])) as ArrayRef),
            ("value", Arc::new(Int32Array::from(vec![0; 3])) as ArrayRef),
            (
                "keep",
                Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut write = table.new_write_builder().new_write().unwrap();
        write.write_arrow_batch(&initial).await.unwrap();
        TableCommit::new(table.clone(), "seed".into())
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
        write.close().await;
        table
    }

    fn numeric_source(keys: Vec<i32>, value: ArrayRef) -> RecordBatch {
        RecordBatch::try_from_iter([
            ("key", Arc::new(Int32Array::from(keys)) as ArrayRef),
            ("value", value),
        ])
        .unwrap()
    }

    async fn numeric_rows(table: &Table) -> Vec<(i64, i32, Option<i32>, Option<i32>)> {
        let mut builder = table.new_read_builder();
        builder
            .with_projection(&["_ROW_ID", "key", "value", "keep"])
            .unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<_> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let keys = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let keep = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    ids.value(row),
                    keys.value(row),
                    (!values.is_null(row)).then(|| values.value(row)),
                    (!keep.is_null(row)).then(|| keep.value(row)),
                )
            }));
        }
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn upsert_key_lookup_uses_btree_row_ranges() {
        let table = numeric_table("memory:/upsert_btree_lookup").await;
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        let mut upsert = TableUpsert::new(
            &table,
            "upsert".into(),
            vec!["key".into()],
            vec!["value".into()],
        )
        .unwrap();
        upsert
            .add_batch(numeric_source(
                vec![1],
                Arc::new(Int32Array::from(vec![42])),
            ))
            .unwrap();
        let source = upsert.key_rows().unwrap();
        let mut matcher = UpsertKeyMatcher::new(&source, upsert.keys.clone()).unwrap();
        let plan = upsert.match_existing(&source, &mut matcher).await.unwrap();
        assert_eq!(plan.snapshot_id(), Some(2));
        assert!(plan
            .splits()
            .iter()
            .all(|split| { split.row_ranges() == Some(&[crate::table::RowRange::new(0, 1)][..]) }));
        let (indices, mut row_ids, new) = matcher.finish();
        row_ids.sort();
        assert_eq!(indices, vec![0, 0]);
        assert_eq!(row_ids, vec![0, 1]);
        assert!(new.is_empty());
        TableCommit::new(table.clone(), "upsert".into())
            .commit(upsert.prepare_commit().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 1, Some(42), Some(10)),
                (1, 1, Some(42), Some(11)),
                (2, 2, Some(0), Some(12)),
            ]
        );
        commit_upsert(
            &table,
            vec![numeric_source(
                vec![9],
                Arc::new(Int32Array::from(vec![49])),
            )],
            vec!["key".into()],
        )
        .await;
        assert_eq!(numeric_rows(&table).await[3], (3, 9, Some(49), None));
    }

    async fn commit_upsert(table: &Table, batches: Vec<RecordBatch>, keys: Vec<String>) {
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = update
            .upsert_by_arrow_with_key(batches, keys)
            .await
            .unwrap();
        TableCommit::new(table.clone(), "upsert".into())
            .commit(messages)
            .await
            .unwrap();
    }

    async fn values_by_row_id(table: &Table) -> Vec<(i64, i32)> {
        let mut builder = table.new_read_builder();
        builder.with_projection(&[ROW_ID, "value"]).unwrap();
        let plan = builder.new_scan().plan_all().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
        }
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn upsert_key_lookup_includes_unindexed_rows_in_fast_mode() {
        for indexed in [false, true] {
            let table = numeric_table(&format!("memory:/upsert_partial_index_{indexed}")).await;
            let table = table.copy_with_options(HashMap::from([(
                "scalar-index.search-mode".into(),
                "fast".into(),
            )]));
            if indexed {
                table
                    .new_global_index_build_builder()
                    .with_index_column("key")
                    .execute()
                    .await
                    .unwrap();
            }
            let appended = RecordBatch::try_from_iter([
                ("key", Arc::new(Int32Array::from(vec![1, 3])) as ArrayRef),
                ("value", Arc::new(Int32Array::from(vec![0, 0])) as ArrayRef),
                ("keep", Arc::new(Int32Array::from(vec![13, 14])) as ArrayRef),
            ])
            .unwrap();
            let mut write = table.new_write_builder().new_write().unwrap();
            write.write_arrow_batch(&appended).await.unwrap();
            TableCommit::new(table.clone(), "append".into())
                .commit(write.prepare_commit().await.unwrap())
                .await
                .unwrap();
            write.close().await;
            commit_upsert(
                &table,
                vec![numeric_source(
                    vec![1, 3, 4],
                    Arc::new(Int32Array::from(vec![41, 43, 44])),
                )],
                vec!["key".into()],
            )
            .await;
            assert_eq!(
                numeric_rows(&table).await,
                vec![
                    (0, 1, Some(41), Some(10)),
                    (1, 1, Some(41), Some(11)),
                    (2, 2, Some(0), Some(12)),
                    (3, 1, Some(41), Some(13)),
                    (4, 3, Some(43), Some(14)),
                    (5, 4, Some(44), None),
                ],
                "indexed={indexed}"
            );
        }
    }

    #[tokio::test]
    async fn upsert_ignore_policy_keeps_unchanged_key_indexes() {
        for update_payload in [false, true] {
            let table =
                numeric_table(&format!("memory:/upsert_ignore_fresh_{update_payload}")).await;
            let table = table.copy_with_options(HashMap::from([(
                "global-index.column-update-action".into(),
                "IGNORE".into(),
            )]));
            table
                .new_global_index_build_builder()
                .with_index_column("key")
                .execute()
                .await
                .unwrap();
            if update_payload {
                let batch = RecordBatch::try_from_iter([
                    (ROW_ID, Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef),
                    (
                        "value",
                        Arc::new(Int32Array::from(vec![11, 12])) as ArrayRef,
                    ),
                ])
                .unwrap();
                let update = table.new_write_builder().new_update().unwrap();
                TableCommit::new(table.clone(), "payload".into())
                    .commit(
                        update
                            .update_by_arrow_with_row_id(vec![batch])
                            .await
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            let (plan, ids, new) = numeric_lookup(&table, 1).await;
            assert!(!plan.splits().is_empty());
            assert!(
                plan.splits().iter().all(|split| {
                    split.row_ranges() == Some(&[crate::table::RowRange::new(0, 1)][..])
                }),
                "payload update={update_payload}"
            );
            assert_eq!(ids, vec![0, 1]);
            assert!(new.is_empty());
        }
    }

    async fn numeric_lookup(table: &Table, key: i32) -> (Plan, Vec<i64>, Vec<usize>) {
        let mut upsert = TableUpsert::new(
            table,
            "upsert".into(),
            vec!["key".into()],
            vec!["value".into()],
        )
        .unwrap();
        upsert
            .add_batch(numeric_source(
                vec![key],
                Arc::new(Int32Array::from(vec![43])),
            ))
            .unwrap();
        let source = upsert.key_rows().unwrap();
        let mut matcher = UpsertKeyMatcher::new(&source, upsert.keys.clone()).unwrap();
        let plan = upsert.match_existing(&source, &mut matcher).await.unwrap();
        let (_, mut ids, new) = matcher.finish();
        ids.sort();
        (plan, ids, new)
    }

    #[tokio::test]
    async fn upsert_checks_index_sources_only_under_ignore() {
        for action in ["IGNORE", "THROW_ERROR", "DROP_PARTITION_INDEX"] {
            for (label, source) in [
                ("missing", None),
                ("malformed", Some(vec![1, 2, 3])),
                (
                    "future",
                    Some(DataEvolutionIndexSourceMeta::new(100).unwrap().serialize()),
                ),
            ] {
                let table = numeric_table(&format!("memory:/upsert_source_{action}_{label}")).await;
                let table = table.copy_with_options(HashMap::from([(
                    "global-index.column-update-action".into(),
                    action.into(),
                )]));
                table
                    .new_global_index_build_builder()
                    .with_index_column("key")
                    .execute()
                    .await
                    .unwrap();
                let manager = table.snapshot_manager();
                let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
                let path = manager.manifest_path(snapshot.index_manifest().unwrap());
                let mut entries = IndexManifest::read(table.file_io(), &path).await.unwrap();
                for entry in &mut entries {
                    entry
                        .index_file
                        .global_index_meta
                        .as_mut()
                        .unwrap()
                        .source_meta = source.clone();
                }
                IndexManifest::write(table.file_io(), &path, &entries)
                    .await
                    .unwrap();
                let (plan, ids, new) = numeric_lookup(&table, 1).await;
                assert!(!plan.splits().is_empty());
                if action == "IGNORE" {
                    assert!(
                        plan.splits()
                            .iter()
                            .all(|split| split.row_ranges().is_none()),
                        "action={action}, source={label}"
                    );
                } else {
                    assert!(
                        plan.splits().iter().all(|split| {
                            split.row_ranges() == Some(&[crate::table::RowRange::new(0, 1)][..])
                        }),
                        "action={action}, source={label}"
                    );
                }
                assert_eq!(ids, vec![0, 1]);
                assert!(new.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn upsert_key_lookup_ignores_stale_indexes_under_ignore_policy() {
        let table = numeric_table("memory:/upsert_ignored_index_updates").await;
        let table = table.copy_with_options(HashMap::from([(
            "global-index.column-update-action".into(),
            "IGNORE".into(),
        )]));
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        let changed_keys = RecordBatch::try_from_iter([
            (ROW_ID, Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef),
            ("key", Arc::new(Int32Array::from(vec![3, 3])) as ArrayRef),
        ])
        .unwrap();
        let update = table.new_write_builder().new_update().unwrap();
        TableCommit::new(table.clone(), "change-keys".into())
            .commit(
                update
                    .update_by_arrow_with_row_id(vec![changed_keys])
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        commit_upsert(
            &table,
            vec![numeric_source(
                vec![3, 4],
                Arc::new(Int32Array::from(vec![43, 44])),
            )],
            vec!["key".into()],
        )
        .await;
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 3, Some(43), Some(10)),
                (1, 3, Some(43), Some(11)),
                (2, 2, Some(0), Some(12)),
                (3, 4, Some(44), None),
            ]
        );
        // Dropping and rebuilding makes the updated keys safe to probe again.
        table
            .new_global_index_drop_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        let (plan, ids, new) = numeric_lookup(&table, 3).await;
        assert!(!plan.splits().is_empty());
        assert!(plan
            .splits()
            .iter()
            .all(|split| { split.row_ranges() == Some(&[crate::table::RowRange::new(0, 1)][..]) }));
        assert_eq!(ids, vec![0, 1]);
        assert!(new.is_empty());
    }

    #[tokio::test]
    async fn upsert_key_lookup_preserves_updated_keys_and_deleted_rows() {
        let table = numeric_table("memory:/upsert_changed_keys").await;
        let table = table.copy_with_options(HashMap::from([
            ("deletion-vectors.enabled".into(), "true".into()),
            (
                "global-index.column-update-action".into(),
                "DROP_PARTITION_INDEX".into(),
            ),
        ]));
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        let changed_key = RecordBatch::try_from_iter([
            (ROW_ID, Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ("key", Arc::new(Int32Array::from(vec![3])) as ArrayRef),
        ])
        .unwrap();
        let update = table.new_write_builder().new_update().unwrap();
        TableCommit::new(table.clone(), "change-key".into())
            .commit(
                update
                    .update_by_arrow_with_row_id(vec![changed_key])
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        commit_upsert(
            &table,
            vec![numeric_source(
                vec![3],
                Arc::new(Int32Array::from(vec![43])),
            )],
            vec!["key".into()],
        )
        .await;
        assert_eq!(numeric_rows(&table).await[2], (2, 3, Some(43), Some(12)));
        // Rebuild so the next lookup encounters an index entry for a deleted row.
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        TableCommit::new(table.clone(), "delete".into())
            .commit(update.delete_by_row_id(vec![0, 1]).await.unwrap())
            .await
            .unwrap();
        commit_upsert(
            &table,
            vec![numeric_source(
                vec![1],
                Arc::new(Int32Array::from(vec![41])),
            )],
            vec!["key".into()],
        )
        .await;
        assert_eq!(
            numeric_rows(&table).await,
            vec![(2, 3, Some(43), Some(12)), (3, 1, Some(41), None)]
        );
    }

    #[tokio::test]
    async fn upsert_composite_btree_preserves_nulls_partitions_and_tuple_identity() {
        let schema = PaimonSchema::builder()
            .column("part", PaimonType::VarChar(VarCharType::string_type()))
            .column("key", PaimonType::Int(IntType::new()))
            .column("tag", PaimonType::VarChar(VarCharType::string_type()))
            .column("value", PaimonType::Int(IntType::new()))
            .partition_keys(["part"])
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert_composite"),
            "memory:/upsert_composite_btree".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let batch = |parts: Vec<&str>,
                     keys: Vec<Option<i32>>,
                     tags: Vec<Option<&str>>,
                     values: Vec<i32>| {
            RecordBatch::try_from_iter([
                ("part", Arc::new(StringArray::from(parts)) as ArrayRef),
                ("key", Arc::new(Int32Array::from(keys)) as ArrayRef),
                ("tag", Arc::new(StringArray::from(tags)) as ArrayRef),
                ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
            ])
            .unwrap()
        };
        let initial = batch(
            vec!["a", "a", "a", "a", "a", "b"],
            vec![Some(1), Some(2), None, Some(1), Some(1), Some(1)],
            vec![Some("x"), Some("y"), Some("x"), None, Some("y"), Some("x")],
            vec![0; 6],
        );
        let mut write = table.new_write_builder().new_write().unwrap();
        write.write_arrow_batch(&initial).await.unwrap();
        TableCommit::new(table.clone(), "seed".into())
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
        write.close().await;
        table
            .new_global_index_build_builder()
            .with_index_columns(&["key", "tag"])
            .execute()
            .await
            .unwrap();
        commit_upsert(
            &table,
            vec![batch(
                vec!["a"; 6],
                vec![Some(1), Some(2), None, Some(1), Some(1), Some(9)],
                vec![Some("x"), Some("y"), Some("x"), None, Some("x"), Some("x")],
                vec![10, 20, 30, 40, 50, 60],
            )],
            vec!["key".into(), "tag".into()],
        )
        .await;
        let mut builder = table.new_read_builder();
        builder
            .with_projection(&["part", "key", "tag", "value"])
            .unwrap();
        let plan = builder.new_scan().plan_all().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let parts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let keys = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let tags = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    parts.value(row).to_string(),
                    (!keys.is_null(row)).then(|| keys.value(row)),
                    (!tags.is_null(row)).then(|| tags.value(row).to_string()),
                    values.value(row),
                )
            }));
        }
        rows.sort();
        // (1, y) is only a cross-product candidate. Partition b must not match.
        let expected = vec![
            ("a", None, Some("x"), 30),
            ("a", Some(1), None, 40),
            ("a", Some(1), Some("x"), 50),
            ("a", Some(1), Some("y"), 0),
            ("a", Some(2), Some("y"), 20),
            ("a", Some(9), Some("x"), 60),
            ("b", Some(1), Some("x"), 0),
        ]
        .into_iter()
        .map(|(part, key, tag, value)| (part.to_string(), key, tag.map(str::to_string), value))
        .collect::<Vec<_>>();
        assert_eq!(rows, expected);
    }

    #[tokio::test]
    async fn upsert_large_composite_domains_keep_index_pruning_and_exact_matches() {
        for large_domain in [false, true] {
            let schema = PaimonSchema::builder()
                .column("key", PaimonType::Int(IntType::new()))
                .column("tag", PaimonType::Int(IntType::new()))
                .column("value", PaimonType::Int(IntType::new()))
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .build()
                .unwrap();
            let table = Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("db", "large_composite_upsert"),
                format!("memory:/upsert_large_composite_domains_{large_domain}"),
                TableSchema::new(0, &schema),
                None,
            );
            let batch = |keys: Vec<i32>, tags: Vec<i32>, value: i32| {
                let count = keys.len();
                RecordBatch::try_from_iter([
                    ("key", Arc::new(Int32Array::from(keys)) as ArrayRef),
                    ("tag", Arc::new(Int32Array::from(tags)) as ArrayRef),
                    (
                        "value",
                        Arc::new(Int32Array::from(vec![value; count])) as ArrayRef,
                    ),
                ])
                .unwrap()
            };
            let (keys, tags, count, stride, matches, candidates) = if large_domain {
                (512, 8, 257, 2, 256, 256)
            } else {
                (128, 32, 17, 4, 16, 17 * 32)
            };
            let mut write = table.new_write_builder().new_write().unwrap();
            write
                .write_arrow_batch(&batch(
                    (0..keys)
                        .flat_map(|key| std::iter::repeat_n(key, tags as usize))
                        .collect(),
                    (0..keys).flat_map(|_| 0..tags).collect(),
                    0,
                ))
                .await
                .unwrap();
            TableCommit::new(table.clone(), "seed".into())
                .commit(write.prepare_commit().await.unwrap())
                .await
                .unwrap();
            write.close().await;
            table
                .new_global_index_build_builder()
                .with_index_columns(&["key", "tag"])
                .execute()
                .await
                .unwrap();

            let mut upsert = TableUpsert::new(
                &table,
                "upsert".into(),
                vec!["key".into(), "tag".into()],
                vec!["value".into()],
            )
            .unwrap();
            // Exercise a large linear IN and a 17 x 17 Cartesian domain.
            // The last tuple is absent; prefix candidates with other suffixes
            // must not be updated or cause an existing match to become an insert.
            upsert
                .add_batch(batch(
                    (0..count).map(|key| key * stride).collect(),
                    (0..count)
                        .map(|i| {
                            if large_domain {
                                7
                            } else if i < matches {
                                i
                            } else {
                                99
                            }
                        })
                        .collect(),
                    42,
                ))
                .unwrap();
            let source = upsert.key_rows().unwrap();
            let mut matcher = UpsertKeyMatcher::new(&source, upsert.keys.clone()).unwrap();
            let plan = upsert.match_existing(&source, &mut matcher).await.unwrap();
            let mut actual_candidates = 0;
            for split in plan.splits() {
                for range in split
                    .row_ranges()
                    .expect("large domains must retain index pruning")
                {
                    actual_candidates += range.to() - range.from() + 1;
                }
            }
            assert_eq!(actual_candidates, candidates);
            let (_, mut matched, new) = matcher.finish();
            matched.sort();
            let expected = (0..matches)
                .map(|key| i64::from(key * stride * tags + if large_domain { 7 } else { key }))
                .collect::<Vec<_>>();
            assert_eq!(matched, expected);
            assert_eq!(new, vec![(count - 1) as usize]);
            TableCommit::new(table.clone(), "upsert".into())
                .commit(upsert.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let rows = values_by_row_id(&table).await;
            assert_eq!(rows.len(), 4097);
            for (row_id, value) in rows {
                let updated = expected.contains(&row_id) || row_id == 4096;
                assert_eq!(
                    value,
                    if updated { 42 } else { 0 },
                    "row {row_id}, large={large_domain}"
                );
            }
        }
    }

    #[tokio::test]
    async fn upsert_blob_keys_fall_back_to_byte_matching() {
        use crate::spec::BlobType;
        use arrow_array::LargeBinaryArray;

        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Blob(BlobType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert_blob"),
            "memory:/upsert_blob_key".into(),
            TableSchema::new(0, &schema),
            None,
        );
        for value in [41, 42] {
            let source = RecordBatch::try_from_iter([
                (
                    "key",
                    Arc::new(LargeBinaryArray::from_vec(vec![b"blob-key"])) as ArrayRef,
                ),
                ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
            ])
            .unwrap();
            commit_upsert(&table, vec![source], vec!["key".into()]).await;
        }
        assert_eq!(values_by_row_id(&table).await, vec![(0, 42)]);
    }

    #[tokio::test]
    async fn upsert_float_keys_keep_arrow_nan_and_signed_zero_identity() {
        use crate::spec::DoubleType;
        use arrow_array::Float64Array;

        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Double(DoubleType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert_float"),
            "memory:/upsert_float_btree".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let batch = |keys: Vec<f64>, values: Vec<i32>| {
            RecordBatch::try_from_iter([
                ("key", Arc::new(Float64Array::from(keys)) as ArrayRef),
                ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
            ])
            .unwrap()
        };
        let mut write = table.new_write_builder().new_write().unwrap();
        write
            .write_arrow_batch(&batch(vec![f64::NAN, -0.0, 0.0, 1.0], vec![0; 4]))
            .await
            .unwrap();
        TableCommit::new(table.clone(), "seed".into())
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
        write.close().await;
        table
            .new_global_index_build_builder()
            .with_index_column("key")
            .execute()
            .await
            .unwrap();
        commit_upsert(
            &table,
            vec![batch(vec![f64::NAN, -0.0], vec![41, 42])],
            vec!["key".into()],
        )
        .await;
        assert_eq!(
            values_by_row_id(&table).await,
            vec![(0, 41), (1, 42), (2, 0), (3, 0)]
        );
    }

    #[tokio::test]
    async fn upsert_casts_surviving_matched_values_and_ignores_other_payloads() {
        let table = numeric_table("memory:/upsert_selected").await;
        let first = numeric_source(vec![1], Arc::new(StringArray::from(vec!["not-an-int"])));
        let mut last = numeric_source(vec![1], Arc::new(StringArray::from(vec!["42"])));
        last = RecordBatch::try_from_iter([
            ("key", last.column(0).clone()),
            ("value", last.column(1).clone()),
            (
                "keep",
                Arc::new(StringArray::from(vec!["unwritten"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = update
            .upsert_by_arrow_with_key(vec![first, last], vec!["key".into()])
            .await
            .unwrap();
        assert!(messages
            .iter()
            .all(|message| message.check_from_snapshot == Some(1)));
        TableCommit::new(table.clone(), "update".into())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 1, Some(42), Some(10)),
                (1, 1, Some(42), Some(11)),
                (2, 2, Some(0), Some(12)),
            ]
        );
    }

    #[tokio::test]
    async fn upsert_keeps_update_casts_separate_from_append_validation() {
        let table = numeric_table("memory:/upsert_mixed_types").await;
        let matched = numeric_source(vec![1], Arc::new(StringArray::from(vec!["42"])));
        let append = numeric_source(vec![3], Arc::new(Int32Array::from(vec![7])));
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = update
            .upsert_by_arrow_with_key(vec![matched, append], vec!["key".into()])
            .await
            .unwrap();
        TableCommit::new(table.clone(), "update".into())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 1, Some(42), Some(10)),
                (1, 1, Some(42), Some(11)),
                (2, 2, Some(0), Some(12)),
                (3, 3, Some(7), None),
            ]
        );
    }

    #[tokio::test]
    async fn upsert_rejects_invalid_appends_before_staging_matched_updates() {
        let table = numeric_table("memory:/upsert_invalid_append").await;
        let batch = numeric_source(vec![1, 3], Arc::new(StringArray::from(vec!["42", "7"])));
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let error = update
            .upsert_by_arrow_with_key(vec![batch], vec!["key".into()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Invalid upsert column 'value'"));
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 1, Some(0), Some(10)),
                (1, 1, Some(0), Some(11)),
                (2, 2, Some(0), Some(12)),
            ]
        );
    }

    #[tokio::test]
    async fn upsert_matched_binary_values_use_existing_row_id_casts() {
        use arrow_array::{BinaryArray, LargeBinaryArray};
        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Int(IntType::new()))
            .column(
                "value",
                PaimonType::VarBinary(crate::spec::VarBinaryType::default()),
            )
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert"),
            "memory:/upsert_binary".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let initial = numeric_source(
            vec![1],
            Arc::new(BinaryArray::from(vec![b"old".as_slice()])),
        );
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer.write_arrow_batch(&initial).await.unwrap();
        TableCommit::new(table.clone(), "seed".into())
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        writer.close().await;
        let update = table.new_write_builder().new_update().unwrap();
        let input = numeric_source(
            vec![1],
            Arc::new(LargeBinaryArray::from(vec![b"\xff\0new".as_slice()])),
        );
        let messages = update
            .upsert_by_arrow_with_key(vec![input], vec!["key".into()])
            .await
            .unwrap();
        TableCommit::new(table.clone(), "update".into())
            .commit(messages)
            .await
            .unwrap();
        let mut builder = table.new_read_builder();
        builder.with_projection(&["value"]).unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<_> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), b"\xff\0new");
    }

    #[tokio::test]
    async fn upsert_matches_integer_key_widths_but_keeps_append_input_types() {
        let table = numeric_table("memory:/upsert_integer_key_widths").await;
        let source = |key, value| {
            RecordBatch::try_from_iter([
                ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
                ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
            ])
            .unwrap()
        };
        let mut update = table.new_write_builder().new_update().unwrap();
        update.with_update_type(vec!["value".into()]).unwrap();
        let messages = update
            .upsert_by_arrow_with_key(vec![source(1, 42)], vec!["key".into()])
            .await
            .unwrap();
        TableCommit::new(table.clone(), "update".into())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(
            numeric_rows(&table).await,
            vec![
                (0, 1, Some(42), Some(10)),
                (1, 1, Some(42), Some(11)),
                (2, 2, Some(0), Some(12))
            ]
        );
        let error = update
            .upsert_by_arrow_with_key(vec![source(3, 7)], vec!["key".into()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Invalid upsert column 'key'"));
        assert_eq!(numeric_rows(&table).await.len(), 3);
    }

    #[tokio::test]
    async fn upsert_normalizes_string_offsets_before_matching_and_writing() {
        use crate::catalog::Identifier;
        use crate::io::FileIOBuilder;
        use crate::spec::{IntType, Schema, TableSchema};
        use crate::table::TableCommit;
        use arrow_array::{Array, Int32Array, LargeStringArray, StringArray};

        let schema = Schema::builder()
            .column("key", PaimonType::VarChar(VarCharType::default()))
            .column("value", PaimonType::VarChar(VarCharType::default()))
            .column("keep", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "upsert"),
            "memory:/upsert".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let mut writer = table
            .new_write_builder()
            .with_commit_user("test")
            .unwrap()
            .new_write()
            .unwrap();
        let initial = RecordBatch::try_from_iter([
            (
                "key",
                Arc::new(StringArray::from(vec![Some("中文"), Some("中文"), None])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["old"; 3])) as ArrayRef,
            ),
            (
                "keep",
                Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&initial).await.unwrap();
        TableCommit::new(table.clone(), "seed".into())
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        writer.close().await;

        let source = RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(LargeStringArray::from(vec![
                    Some("discarded"),
                    Some(""),
                    None,
                    Some("final"),
                ])) as ArrayRef,
            ),
            (
                "key",
                Arc::new(LargeStringArray::from(vec![
                    Some("中文"),
                    Some("new"),
                    None,
                    Some("中文"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut updater = table.new_write_builder().new_update().unwrap();
        updater.with_update_type(vec!["value".into()]).unwrap();
        let messages = updater
            .upsert_by_arrow_with_key(
                vec![source.slice(0, 1), source.slice(1, 3)],
                vec!["key".into()],
            )
            .await
            .unwrap();
        TableCommit::new(table.clone(), "upsert".into())
            .commit(messages)
            .await
            .unwrap();
        let mut reader = table.new_read_builder();
        reader
            .with_projection(&["key", "value", "keep", "_ROW_ID"])
            .unwrap();
        let plan = reader.new_scan().plan().await.unwrap();
        let mut batches = reader.new_read().unwrap().to_arrow(plan.splits()).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await.unwrap() {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let keep = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    ids.value(row),
                    (!keys.is_null(row)).then(|| keys.value(row).to_string()),
                    (!values.is_null(row)).then(|| values.value(row).to_string()),
                    (!keep.is_null(row)).then(|| keep.value(row)),
                )
            }));
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (0, Some("中文".into()), Some("final".into()), Some(10)),
                (1, Some("中文".into()), Some("final".into()), Some(11)),
                (2, None, None, Some(12)),
                (3, Some("new".into()), Some("".into()), None),
            ]
        );
    }

    #[tokio::test]
    async fn upsert_finds_java_padded_null_timestamp_partition() {
        let schema = PaimonSchema::builder()
            .column(
                "part",
                PaimonType::Timestamp(TimestampType::new(6).unwrap()),
            )
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .partition_keys(["part"])
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "padded_null_partition"),
            "memory:/upsert_padded_null_partition".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let batch = |value| {
            RecordBatch::try_from_iter([
                (
                    "part",
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                        None::<i64>,
                    ])) as ArrayRef,
                ),
                ("key", Arc::new(Int32Array::from(vec![7])) as ArrayRef),
                ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
            ])
            .unwrap()
        };
        let mut write = table.new_write_builder().new_write().unwrap();
        write.write_arrow_batch(&batch(10)).await.unwrap();
        let mut messages = write.prepare_commit().await.unwrap();
        for message in &mut messages {
            let row = BinaryRow::from_serialized_bytes(&message.partition).unwrap();
            let mut padded = row.data().to_vec();
            // Java reserves a variable slot for NULL TIMESTAMP(6), although
            // the null bit makes its offset irrelevant to the logical value.
            padded.extend_from_slice(&[0; 8]);
            padded[8..16].copy_from_slice(&(16_u64 << 32).to_le_bytes());
            message.partition = BinaryRow::from_bytes(1, padded).to_serialized_bytes();
        }
        TableCommit::new(table.clone(), "seed".into())
            .commit(messages)
            .await
            .unwrap();
        write.close().await;
        commit_upsert(&table, vec![batch(42)], vec!["key".into()]).await;
        assert_eq!(values_by_row_id(&table).await, vec![(0, 42)]);
    }
}
