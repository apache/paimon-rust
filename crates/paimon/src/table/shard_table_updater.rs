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

//! Positional column updates over a shard of complete Data Evolution file groups.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_stream::try_stream;
use futures::TryStreamExt;

use crate::spec::{row_id_data_field, DataField};

use super::data_evolution_writer::RowIdFileIndex;
use super::source::is_data_evolution_normal_file;
use super::update_input::{cast_update_value, unique_column_index, CastMode};
use super::{ArrowRecordBatchStream, CommitMessage, DataEvolutionWriter, DataSplit, Table};

const ROW_ID: &str = "_ROW_ID";

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[derive(Default)]
struct ReadState {
    pending: VecDeque<i64>,
    complete: bool,
    failed: bool,
    closed: bool,
}

/// Read and replace columns of one balanced shard without exposing row-ID mapping
/// to the caller. The read uses one captured snapshot. Updates follow reader order
/// and may use different batch boundaries; deleted rows never consume input values.
///
/// Finish the reader and supply exactly one output row per visible input row before
/// preparing. Core reuses the row-ID update writer, preserving Java's column-file
/// metadata, original physical ranges and conflict checks. Buffer one file group
/// at a time; returned messages belong to the caller and are never removed on close.
pub struct ShardTableUpdater {
    table: Table,
    splits: Vec<DataSplit>,
    read_type: Vec<DataField>,
    write_schema: SchemaRef,
    index: RowIdFileIndex,
    writer: Option<DataEvolutionWriter>,
    write_columns: Vec<String>,
    current_group: Option<i64>,
    finished_groups: HashSet<i64>,
    staged: Vec<CommitMessage>,
    blob_uri_reader_factory: Option<Arc<dyn crate::io::UriReaderFactory>>,
    state: Arc<Mutex<ReadState>>,
    reader_started: bool,
    failed: bool,
    messages: Option<Vec<CommitMessage>>,
}

impl ShardTableUpdater {
    pub(super) async fn new(
        table: &Table,
        read_type: Option<Vec<DataField>>,
        write_columns: Vec<String>,
        shard: usize,
        shard_count: usize,
    ) -> crate::Result<Self> {
        if shard_count == 0 || shard >= shard_count {
            return Err(invalid(
                "Shard index must be smaller than a positive shard count",
            ));
        }
        if write_columns.is_empty() {
            return Err(invalid("Shard update columns must not be empty"));
        }
        let writer = DataEvolutionWriter::new(table, write_columns.clone())?;
        let snapshot = super::time_travel::resolve_snapshot(table).await?;
        let table = table
            .copy_with_options(HashMap::from([(
                "scalar-index.search-mode".into(),
                "FULL".into(),
            )]))
            .copy_with_pinned_snapshot(snapshot.as_ref());
        let plan = table.new_read_builder().new_scan().plan_all_files().await?;
        let splits = select_shard(plan.splits(), shard, shard_count)?;
        let index = RowIdFileIndex::from_splits(table.clone(), &splits)?;
        let read_type = read_type.unwrap_or_else(|| table.schema().fields().to_vec());
        // Validate the same canonical request used by ordinary table readers.
        table
            .new_read_builder()
            .with_read_type(with_row_id(&read_type)?)
            .new_read()?;
        let fields = super::data_evolution_fields::project_by_paths(
            table.schema().fields(),
            &write_columns,
        )?;
        let write_schema = crate::arrow::build_target_arrow_schema(&fields)?;
        let state = ReadState {
            complete: splits.is_empty(),
            ..Default::default()
        };
        Ok(Self {
            table,
            splits,
            read_type,
            write_schema,
            index,
            writer: Some(writer),
            write_columns,
            current_group: None,
            finished_groups: HashSet::new(),
            staged: Vec::new(),
            blob_uri_reader_factory: None,
            state: Arc::new(Mutex::new(state)),
            reader_started: false,
            failed: false,
            messages: None,
        })
    }

    fn ensure_active(&self) -> crate::Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid("Shard reader state poisoned"))?;
        if state.closed || state.failed || self.failed || self.messages.is_some() {
            return Err(invalid(
                "Shard updater is closed, failed or already prepared",
            ));
        }
        Ok(())
    }

    pub(super) fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<Arc<dyn crate::io::UriReaderFactory>>,
    ) {
        self.writer
            .as_mut()
            .unwrap()
            .with_blob_uri_reader_factory(factory.clone());
        self.blob_uri_reader_factory = factory;
    }

    /// Open the shard's single reader. Only requested columns are returned;
    /// `_ROW_ID` is retained internally even when absent from the read type.
    pub fn arrow_reader(&mut self) -> crate::Result<ArrowRecordBatchStream> {
        self.ensure_active()?;
        if self.reader_started {
            return Err(invalid("Shard reader can only be opened once"));
        }
        let mut builder = self.table.new_read_builder();
        builder.with_read_type(with_row_id(&self.read_type)?);
        let mut input = builder.new_read()?.for_update().to_arrow(&self.splits)?;
        let state = self.state.clone();
        let output_columns = self.read_type.len();
        self.reader_started = true;
        Ok(Box::pin(try_stream! {
            loop {
                let next = input.try_next().await;
                if next.is_err() { finish_read(&state, true)?; }
                let Some(batch) = next? else { finish_read(&state, false)?; break; };
                let output = record_rows(&state, batch, output_columns);
                if output.is_err() { finish_read(&state, true)?; }
                yield output?;
            }
        }))
    }

    /// Supply column values in visible reader order. Validate the complete batch
    /// before consuming any row IDs, so a malformed batch can be corrected.
    pub async fn update_by_arrow_batch(&mut self, data: RecordBatch) -> crate::Result<()> {
        self.ensure_active()?;
        if data.num_columns() != self.write_schema.fields().len() {
            return Err(invalid(
                "Shard input must contain exactly the configured update columns",
            ));
        }
        let mut columns = Vec::<ArrayRef>::new();
        for field in self.write_schema.fields() {
            let index = unique_column_index(data.schema().as_ref(), field.name())?;
            columns.push(cast_update_value(
                data.column(index),
                field.data_type(),
                CastMode::Assignment,
            )?);
        }
        let ids = {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid("Shard reader state poisoned"))?;
            if data.num_rows() > state.pending.len() {
                return Err(invalid(
                    "Shard update contains more rows than have been read",
                ));
            }
            state
                .pending
                .iter()
                .take(data.num_rows())
                .copied()
                .collect::<Vec<_>>()
        };
        // Resolve all group boundaries before staging or consuming this input.
        let groups = ids
            .iter()
            .map(|id| self.index.first_row_id_for(*id))
            .collect::<crate::Result<Vec<_>>>()?;
        let mut arrays = vec![Arc::new(Int64Array::from(ids)) as ArrayRef];
        arrays.extend(columns);
        let mut fields = vec![Arc::new(Field::new(ROW_ID, DataType::Int64, false))];
        fields.extend(self.write_schema.fields().iter().cloned());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| invalid(error.to_string()))?;
        let result = self.stage_groups(batch, &groups).await;
        if result.is_err() {
            self.failed = true;
        }
        result?;
        self.state
            .lock()
            .map_err(|_| invalid("Shard reader state poisoned"))?
            .pending
            .drain(..data.num_rows());
        Ok(())
    }

    /// Prepare after complete consumption. Repeated calls return the same messages.
    pub async fn prepare_commit(&mut self) -> crate::Result<Vec<CommitMessage>> {
        if let Some(messages) = &self.messages {
            return Ok(messages.clone());
        }
        self.ensure_active()?;
        {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid("Shard reader state poisoned"))?;
            if !state.complete || !state.pending.is_empty() {
                return Err(invalid("Read and update exactly the same number of rows from this shard before preparing"));
            }
        }
        let result = self.flush_group().await;
        if result.is_err() {
            self.failed = true;
        }
        result?;
        let messages = std::mem::take(&mut self.staged);
        self.messages = Some(messages.clone());
        Ok(messages)
    }

    async fn stage_groups(&mut self, batch: RecordBatch, groups: &[i64]) -> crate::Result<()> {
        let mut offset = 0;
        while offset < groups.len() {
            let group = groups[offset];
            if self.current_group.is_some_and(|current| current != group) {
                self.flush_group().await?;
            }
            if self.finished_groups.contains(&group) {
                return Err(invalid(
                    "Shard reader revisited an already updated file group",
                ));
            }
            if self.writer.is_none() {
                let mut writer = DataEvolutionWriter::new(&self.table, self.write_columns.clone())?;
                writer.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
                self.writer = Some(writer);
            }
            self.current_group = Some(group);
            let count = groups[offset..]
                .iter()
                .take_while(|id| **id == group)
                .count();
            self.writer
                .as_mut()
                .unwrap()
                .add_matched_batch(batch.slice(offset, count))?;
            offset += count;
        }
        Ok(())
    }

    async fn flush_group(&mut self) -> crate::Result<()> {
        if let Some(writer) = self.writer.take() {
            // Cancellation also drops this consuming writer. Poison first so a
            // cancelled prepare/update cannot later return a partial success.
            self.failed = true;
            self.staged
                .extend(writer.prepare_commit_with_index(&self.index).await?);
            self.failed = false;
        }
        if let Some(group) = self.current_group.take() {
            self.finished_groups.insert(group);
        }
        Ok(())
    }

    /// Stop this operation. Prepared CommitMessage files may already be published.
    pub fn close(&mut self) {
        self.writer.take();
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.pending.clear();
        }
    }
}

/// The caller's read fields must not change the system column used for mapping.
fn with_row_id(read_type: &[DataField]) -> crate::Result<Vec<DataField>> {
    let row_id = row_id_data_field();
    let mut ids = read_type.iter().filter(|field| field.name() == ROW_ID);
    if let Some(field) = ids.next() {
        if ids.next().is_some()
            || field.id() != row_id.id()
            || !matches!(field.data_type(), crate::spec::DataType::BigInt(_))
        {
            return Err(invalid(
                "Read type must use the canonical BIGINT _ROW_ID field",
            ));
        }
        return Ok(read_type.to_vec());
    }
    let mut fields = read_type.to_vec();
    fields.push(row_id);
    Ok(fields)
}

fn select_shard(splits: &[DataSplit], shard: usize, count: usize) -> crate::Result<Vec<DataSplit>> {
    let mut ranges = splits
        .iter()
        .flat_map(|split| split.data_files())
        .filter(|file| is_data_evolution_normal_file(file))
        .map(|file| {
            file.row_id_range()
                .ok_or_else(|| invalid("Data Evolution file is missing its row-ID range"))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    ranges.sort_unstable();
    let mut groups = Vec::<(i64, i64)>::new();
    for (start, end) in ranges {
        if let Some(last) = groups.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            groups.push((start, end));
        }
    }
    let size = groups.len() / count;
    let remainder = groups.len() % count;
    let begin = shard * size + shard.min(remainder);
    let end = begin + size + usize::from(shard < remainder);
    let Some(&(first, _)) = groups.get(begin).filter(|_| begin < end) else {
        return Ok(Vec::new());
    };
    let last = groups[end - 1].1;
    // The selected groups are a contiguous slice in global row-ID order.
    // One interval avoids a scan of every selected group for each split.
    Ok(splits
        .iter()
        .filter(|split| {
            split
                .data_files()
                .iter()
                .filter(|file| is_data_evolution_normal_file(file))
                .filter_map(|file| file.row_id_range())
                .any(|(start, end)| first <= end && last >= start)
        })
        .map(|split| {
            split
                .clone()
                .with_selected_row_ranges(vec![super::RowRange::new(first, last)])
        })
        .collect())
}

fn record_rows(
    state: &Mutex<ReadState>,
    batch: RecordBatch,
    output_columns: usize,
) -> crate::Result<RecordBatch> {
    let ids = batch
        .column_by_name(ROW_ID)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| invalid("Shard reader must produce Int64 row IDs"))?;
    if ids.null_count() != 0 {
        return Err(invalid("Shard reader produced NULL row IDs"));
    }
    let output = batch
        .project(&(0..output_columns).collect::<Vec<_>>())
        .map_err(|error| invalid(error.to_string()))?;
    let mut state = state
        .lock()
        .map_err(|_| invalid("Shard reader state poisoned"))?;
    if state.closed {
        return Err(invalid("Shard updater was closed"));
    }
    state.pending.extend(ids.values().iter().copied());
    Ok(output)
}

fn finish_read(state: &Mutex<ReadState>, failed: bool) -> crate::Result<()> {
    let mut state = state
        .lock()
        .map_err(|_| invalid("Shard reader state poisoned"))?;
    state.failed |= failed;
    state.complete = !state.failed;
    Ok(())
}

#[cfg(test)]
#[path = "shard_table_updater/tests.rs"]
mod tests;
