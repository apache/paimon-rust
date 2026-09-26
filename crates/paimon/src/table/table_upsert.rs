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

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{concat::concat_batches, take::take};
use futures::TryStreamExt;

use crate::spec::CoreOptions;
use crate::table::{CommitMessage, Table, UpsertKeyMatcher};

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

/// Upsert full Arrow rows into a data-evolution table without primary keys.
/// Existing keys are updated by row ID; new keys are appended. The caller
/// commits the returned messages through the same write builder's committer.
#[must_use = "upsert must be used to call prepare_commit()"]
pub struct TableUpsert {
    table: Table,
    commit_user: String,
    keys: Vec<String>,
    update_columns: Vec<String>,
    source: Vec<RecordBatch>,
}

impl TableUpsert {
    pub(crate) fn new(
        table: &Table,
        commit_user: String,
        keys: Vec<String>,
        update_columns: Vec<String>,
    ) -> crate::Result<Self> {
        if keys.is_empty() {
            return Err(invalid("upsert keys must not be empty"));
        }
        if !table.schema().partition_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "native upsert currently requires an unpartitioned table".to_string(),
            });
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
        let _validated_update = super::TableUpdateByRowId::new(table, update_columns.clone())?;
        Ok(Self {
            table: table.clone(),
            commit_user,
            keys,
            update_columns,
            source: Vec::new(),
        })
    }

    /// Add full rows. Column order may differ from the table schema; names and
    /// Arrow types must agree. Multiple batches form one logical upsert input.
    pub fn add_batch(&mut self, batch: RecordBatch) -> crate::Result<()> {
        let target = crate::arrow::build_target_arrow_schema(self.table.schema().fields())?;
        if batch.num_columns() != target.fields().len() {
            return Err(invalid("native upsert requires all table columns"));
        }
        let mut columns = Vec::with_capacity(target.fields().len());
        for field in target.fields() {
            let column = batch
                .column_by_name(field.name())
                .ok_or_else(|| invalid(format!("missing upsert column '{}'", field.name())))?;
            if column.data_type() != field.data_type() {
                return Err(invalid(format!(
                    "upsert column '{}' type differs from table: {:?} != {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
            columns.push(column.clone());
        }
        let ordered = RecordBatch::try_new(target, columns)
            .map_err(|error| invalid(format!("cannot order upsert columns: {error}")))?;
        self.source.push(ordered);
        Ok(())
    }

    /// Match source keys against the target snapshot, stage row-ID updates and
    /// new rows, then return both sets of commit messages as one operation.
    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(self) -> crate::Result<Vec<CommitMessage>> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let Some(first) = self.source.first() else {
            return Err(invalid("Input data is empty"));
        };
        let source = concat_batches(&first.schema(), &self.source)
            .map_err(|error| invalid(format!("cannot concatenate upsert input: {error}")))?;
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
        let mut stream = read.to_arrow(plan.splits())?;
        while let Some(batch) = stream.try_next().await? {
            matcher.add_existing_batch(&batch)?;
        }
        let (matched_indices, row_ids, new_indices) = matcher.finish();

        let mut messages = Vec::new();
        let result: crate::Result<()> = async {
            if !matched_indices.is_empty() {
                let selected = selected_rows(&source, &matched_indices)?;
                let mut fields = selected.schema().fields().to_vec();
                fields.push(Arc::new(Field::new(ROW_ID, DataType::Int64, false)));
                let mut columns: Vec<ArrayRef> = selected.columns().to_vec();
                columns.push(Arc::new(Int64Array::from(row_ids)));
                let matched = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .map_err(|error| {
                        invalid(format!("cannot build matched upsert rows: {error}"))
                    })?;
                let mut update = self
                    .table
                    .new_write_builder()
                    .with_commit_user(self.commit_user.clone())?
                    .new_update_by_row_id(self.update_columns)?;
                if let Some(snapshot_id) = plan.snapshot_id() {
                    update.pin_read_snapshot(snapshot_id);
                }
                update.add_matched_batch(matched)?;
                messages.extend(update.prepare_commit().await?);
            }
            if !new_indices.is_empty() {
                let new_rows = selected_rows(&source, &new_indices)?;
                let mut append = self
                    .table
                    .new_write_builder()
                    .with_commit_user(self.commit_user.clone())?
                    .new_write()?;
                if let Err(error) = append.write_arrow_batch(&new_rows).await {
                    append.close().await;
                    return Err(error);
                }
                messages.extend(append.prepare_commit().await?);
            }
            Ok(())
        }
        .await;
        if result.is_err() && !messages.is_empty() {
            if let Ok(builder) = self
                .table
                .new_write_builder()
                .with_commit_user(self.commit_user)
            {
                let _ = builder.new_commit().abort(&messages).await;
            }
        }
        result.map(|()| messages)
    }
}
