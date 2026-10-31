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

//! Adapt a complete PK level to the shared native index build pipeline.
use super::super::super::invalid;
use crate::resource::ResourceContext;
use crate::spec::{DataField, DataFileMeta, GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME};
use crate::table::index_build::{
    self, full_text,
    scalar::{self, ScalarIndexOptions},
    vector, IndexFileWriter,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::pk_index_maintenance::{row_count, source_meta};
use crate::table::pk_index_options::Definition;
use crate::table::{ArrowRecordBatchStream, DataSplit, RowRange, Table};
use crate::vindex::VindexVectorIndexOptions;
use crate::Result;
use arrow_array::{new_null_array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use async_stream::try_stream;
use futures::TryStreamExt;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

fn with_ordinals(batch: RecordBatch, start: i64) -> Result<RecordBatch> {
    let end = start
        .checked_add(batch.num_rows() as i64)
        .ok_or_else(|| invalid("PK index ordinal overflow"))?;
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        ROW_ID_FIELD_NAME,
        ArrowType::Int64,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from_iter_values(start..end)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(index_build::error)
}

/// Select live vector positions before casts/native validation. Reinsert bounded
/// NULL batches for excluded positions so every native writer keeps the same
/// dense physical ordinal and source-count contract.
fn batches(
    table: Table,
    split: DataSplit,
    files: Vec<DataFileMeta>,
    field: DataField,
    deletions: Arc<HashMap<String, RoaringBitmap>>,
    resources: Option<ResourceContext>,
) -> ArrowRecordBatchStream {
    Box::pin(try_stream! {
        let mut offset = 0i64;
        let read_size = table.schema().core_options().read_batch_size()?.max(1);
        let null_schema = crate::arrow::build_read_arrow_schema(std::slice::from_ref(&field))?;
        let null_schema = Arc::new(Schema::new(vec![null_schema.field(0).clone().with_nullable(true)]));
        for file in &files {
            let whole = if file.row_count == 0 { vec![] } else { vec![RowRange::new(0, file.row_count - 1)] };
            let ranges = if let Some(mask) = deletions.get(&file.file_name) {
                let mut deleted = Vec::new();
                let mut positions = mask.iter();
                while let Some(range) = positions.next_range() {
                    deleted.push(RowRange::new(i64::from(*range.start()), i64::from(*range.end())));
                }
                crate::table::source::exclude_row_ranges(&whole, &deleted)
            } else { whole };
            let reader = super::super::new_reader_with_projection(
                &table, resources.clone(), vec![field.clone()],
            )?;
            let selection = deletions.contains_key(&file.file_name).then(|| ranges.clone());
            let mut stream = reader.read_physical_file_with_ranges(split.clone(), file.clone(), selection)?;
            let mut pending: Option<RecordBatch> = None;
            let mut position = 0i64;
            for range in ranges {
                while position < range.from() {
                    let rows = (range.from() - position).min(read_size as i64) as usize;
                    let batch = RecordBatch::try_new(null_schema.clone(), vec![new_null_array(null_schema.field(0).data_type(), rows)])
                        .map_err(index_build::error)?;
                    yield with_ordinals(batch, offset + position)?;
                    position += rows as i64;
                }
                while position <= range.to() {
                    let batch = match pending.take() {
                        Some(batch) => batch,
                        None => stream.try_next().await?.ok_or_else(|| invalid("PK index source ended before its selected rows"))?,
                    };
                    if batch.num_rows() == 0 { continue; }
                    let rows = batch.num_rows().min((range.to() - position + 1) as usize);
                    yield with_ordinals(batch.slice(0, rows), offset + position)?;
                    position += rows as i64;
                    if rows < batch.num_rows() {
                        pending = Some(batch.slice(rows, batch.num_rows() - rows));
                    }
                }
            }
            while position < file.row_count {
                let rows = (file.row_count - position).min(read_size as i64) as usize;
                let batch = RecordBatch::try_new(null_schema.clone(), vec![new_null_array(null_schema.field(0).data_type(), rows)])
                    .map_err(index_build::error)?;
                yield with_ordinals(batch, offset + position)?;
                position += rows as i64;
            }
            if pending.is_some() || stream.try_next().await?.is_some() {
                Err(invalid("PK index source exceeds its selected rows"))?;
            }
            offset = offset.checked_add(position).ok_or_else(|| invalid("PK index ordinal overflow"))?;
        }
        if offset != row_count(&files)? {
            Err(invalid("PK index source row count changed"))?;
        }
    })
}

pub(super) async fn build(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    definition: &Definition,
    deletions: Arc<HashMap<String, RoaringBitmap>>,
    resources: Option<ResourceContext>,
) -> Result<Option<IndexFileMeta>> {
    let count = row_count(files)?;
    let range = RowRange::new(0, count - 1);
    let meta = GlobalIndexMeta {
        row_range_start: range.from(),
        row_range_end: range.to(),
        index_field_id: definition.field.id(),
        extra_field_ids: None,
        source_meta: Some(source_meta(files[0].level, files)?.serialize()?),
        index_meta: None,
    };
    let in_data = table.schema().core_options().index_file_in_data_file_dir();
    let relative = split
        .bucket_path()
        .strip_prefix(&table.data_file_location())
        .unwrap_or(split.bucket_path())
        .trim_start_matches('/');
    let file = IndexFileWriter::new(
        table,
        &definition.index_type,
        format!(
            "{}-pk-index-{}.index",
            definition.index_type,
            uuid::Uuid::new_v4()
        ),
        IndexFileLocation::BucketLocal {
            table_path: table.location().trim_end_matches('/'),
            bucket_path: split.bucket_path(),
            index_file_in_data_file_dir: in_data,
        },
        relative,
        meta,
    )
    .await?;
    let deletions = if crate::vindex::is_vindex_index_type(&definition.index_type) {
        deletions
    } else {
        Arc::default()
    };
    let stream = batches(
        table.clone(),
        split.clone(),
        files.to_vec(),
        definition.field.clone(),
        deletions,
        resources,
    );
    let mut options = table.schema().options().clone();
    options.extend(definition.options.clone());
    file.write(table, |output| async move {
        match definition.index_type.as_str() {
            "full-text" => {
                let native = full_text::native_full_text_options(&options);
                full_text::write_payload(
                    stream,
                    output,
                    definition.field.name(),
                    range,
                    &native,
                    index_build::serialize_options(&native)?,
                )
                .await
            }
            kind if crate::vindex::is_vindex_index_type(kind) => {
                let native = VindexVectorIndexOptions::new(
                    table.schema().options(),
                    &definition.options,
                    kind,
                    &definition.field,
                )?;
                let payload = vector::write_payload(
                    stream,
                    output.clone(),
                    definition.field.name(),
                    &native,
                    range,
                    index_build::serialize_options(&native.native_options)?,
                )
                .await?;
                let payload = match payload {
                    Some(payload) => payload,
                    None => {
                        // Java persists an empty segment to record completed
                        // coverage; readers never open its native payload.
                        output.write(bytes::Bytes::new()).await?;
                        index_build::IndexPayload {
                            row_count: 0,
                            index_meta: Vec::new(),
                        }
                    }
                };
                Ok(Some(payload))
            }
            kind => scalar::write_payload(
                stream,
                output,
                &[&definition.field],
                range,
                &ScalarIndexOptions::from_options(kind, &options)?,
            )
            .await
            .map(Some),
        }
    })
    .await
}
