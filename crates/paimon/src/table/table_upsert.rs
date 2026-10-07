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
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat_batches, take::take};
use futures::TryStreamExt;
use indexmap::IndexMap;

use super::update_input::unique_column_index;
use super::upsert_key_matcher::UpsertKeyMatcher;
use super::write_batch_normalize::normalize_write_array;
use crate::spec::{
    batch_to_serialized_bytes, extract_datum, BinaryRow, BinaryRowBuilder, CoreOptions, DataField,
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

// Java may reserve variable-length storage for NULL timestamps/decimals.
// Compare logical partitions using one encoding, independent of that padding.
fn partition_key(row: &BinaryRow, fields: &[DataField]) -> crate::Result<Vec<u8>> {
    let mut builder = BinaryRowBuilder::new(fields.len() as i32);
    for (index, field) in fields.iter().enumerate() {
        match extract_datum(row, index, field.data_type())? {
            Some(value) => builder.write_datum(index, &value, field.data_type()),
            None => builder.set_null_at(index),
        }
    }
    Ok(builder.build().to_serialized_bytes())
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
        })
    }

    /// Add rows containing the keys and columns to write. Column order may
    /// differ from the table schema; names and Arrow layouts follow the same
    /// normalization as ordinary writes. Batches form one logical upsert input,
    /// but may carry different non-key columns, preserving absent fields.
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
            columns.push(
                normalize_write_array(column, field.data_type()).map_err(|error| {
                    invalid(format!("Invalid upsert column '{}': {error}", field.name()))
                })?,
            );
            fields.push(field.clone());
        }
        let ordered = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| invalid(format!("cannot order upsert columns: {error}")))?;
        self.source.push(ordered);
        Ok(())
    }

    /// Concatenate only match keys. Padding payload columns with NULL would
    /// lose the distinction between an absent field and an explicit NULL.
    fn key_rows(&self) -> crate::Result<RecordBatch> {
        let batches = self
            .source
            .iter()
            .map(|batch| {
                let columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| self.keys.contains(field.name()))
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                batch
                    .project(&columns)
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
            if partitions.contains(&partition_key(split.partition(), &fields)?) {
                splits.push(split.clone());
            }
        }
        Ok(Cow::Owned(splits))
    }

    /// Match source keys against the target snapshot, stage row-ID updates and
    /// new rows, then return both sets of commit messages as one operation.
    #[must_use = "commit messages must be passed to TableCommit"]
    pub(super) async fn prepare_commit(self) -> crate::Result<Vec<CommitMessage>> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let source = self.key_rows()?;
        if source.num_rows() == 0 {
            return Err(invalid("Input data is empty"));
        }
        let mut matcher = UpsertKeyMatcher::new(&source, self.keys.clone())?;

        let mut read_builder = self.table.new_read_builder();
        let mut projection = self.keys.iter().map(String::as_str).collect::<Vec<_>>();
        projection.push(ROW_ID);
        read_builder.with_projection(&projection)?;
        let plan = read_builder.new_scan().plan().await?;
        let read = read_builder.new_read()?;
        let splits = self.matching_splits(&source, &plan)?;
        let mut stream = read.to_arrow(splits.as_ref())?;
        while let Some(batch) = stream.try_next().await? {
            matcher.add_existing_batch(&batch)?;
        }
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
                if let Some(snapshot_id) = plan.snapshot_id() {
                    update.pin_read_snapshot(snapshot_id);
                }
                for (selected, range) in matched_batches {
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
    use crate::spec::{DataType as PaimonType, DecimalType, TimestampType, VarCharType};

    #[test]
    fn partition_identity_ignores_java_null_variable_storage() {
        let fields = vec![
            DataField::new(
                0,
                "ts".into(),
                PaimonType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                1,
                "amount".into(),
                PaimonType::Decimal(DecimalType::new(20, 2).unwrap()),
            ),
            DataField::new(
                2,
                "label".into(),
                PaimonType::VarChar(VarCharType::new(100).unwrap()),
            ),
        ];
        let mut builder = BinaryRowBuilder::new(3);
        builder.set_null_at(0);
        builder.set_null_at(1);
        builder.write_string(2, "after-null-fields");
        let canonical = builder.build();
        let mut java = canonical.data()[..32].to_vec();
        // BinaryWriter.writeTimestamp(NULL, 6) reserves 8 bytes;
        // writeDecimal(NULL, 20) reserves another 16, even though both are NULL.
        java.extend_from_slice(&[0; 24]);
        java.extend_from_slice(&canonical.data()[32..]);
        java[8..16].copy_from_slice(&(32_u64 << 32).to_le_bytes());
        java[16..24].copy_from_slice(&(40_u64 << 32).to_le_bytes());
        let offset_and_size = u64::from_le_bytes(canonical.data()[24..32].try_into().unwrap());
        java[24..32].copy_from_slice(&(offset_and_size + (24_u64 << 32)).to_le_bytes());
        let java = BinaryRow::from_bytes(3, java);
        assert_ne!(java.to_serialized_bytes(), canonical.to_serialized_bytes());
        assert_eq!(
            partition_key(&java, &fields).unwrap(),
            canonical.to_serialized_bytes()
        );
    }
}
