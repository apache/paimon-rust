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

//! Sparse Blob updates compatible with Java BlobFallbackRecordReader.

use super::{matched_column, FileRowRange};
use crate::spec::{CoreOptions, DataField, DataFileMeta};
use crate::table::Table;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_select::interleave::interleave;
use std::collections::HashSet;
use std::sync::Arc;

pub(super) struct BlobUpdateBatch {
    pub field: DataField,
    pub batch: RecordBatch,
    pub updated_rows: Option<Vec<usize>>,
}

/// The committer assigns one snapshot sequence to the entire Blob delta.
pub(super) fn assign_delta_metadata(mut first_row_id: i64, files: &mut [DataFileMeta]) {
    for file in files {
        file.first_row_id = Some(first_row_id);
        first_row_id += file.row_count;
        file.min_sequence_number = 0;
        file.max_sequence_number = 0;
    }
}

pub(super) async fn file_updates(
    table: &Table,
    fields: &[DataField],
    batches: &[RecordBatch],
    matches: &[(usize, usize, usize)],
    file: &FileRowRange,
) -> Result<Vec<BlobUpdateBatch>> {
    let inline = CoreOptions::new(table.schema().options()).blob_inline_fields();
    let fields = fields
        .iter()
        .filter(|field| is_dedicated_blob(field, &inline))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    let baseline = baseline_field_ids(table, &file.files).await?;
    fields
        .into_iter()
        .map(|field| {
            let (batch, updated_rows) = delta_batch(
                field,
                batches,
                matches,
                file.row_count as usize,
                baseline.contains(&field.id()),
            )?;
            Ok(BlobUpdateBatch {
                field: field.clone(),
                batch,
                updated_rows,
            })
        })
        .collect()
}

pub(super) fn is_dedicated_blob(field: &DataField, inline: &HashSet<String>) -> bool {
    field.data_type().is_blob_file_field() && !inline.contains(field.name())
}

/// The ordinary-column rewrite never requests dedicated Blob payloads. Keep
/// their independently rolled ranges out of its physical merge plan as well.
pub(super) fn normal_read_files(files: &[DataFileMeta]) -> Vec<DataFileMeta> {
    files
        .iter()
        .filter(|file| {
            !crate::table::dedicated_format_file_writer::is_blob_or_video_file_name(&file.file_name)
        })
        .cloned()
        .collect()
}

/// Resolve providers by field ID, so a dropped column's files cannot supply
/// a subsequently added Blob column with the same name.
pub(super) async fn baseline_field_ids(
    table: &Table,
    files: &[DataFileMeta],
) -> Result<HashSet<i32>> {
    let mut ids = HashSet::new();
    for file in files {
        if !file.file_name.to_ascii_lowercase().ends_with(".blob") {
            continue;
        }
        let columns = file
            .write_cols
            .as_ref()
            .filter(|columns| columns.len() == 1)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "Blob file '{}' must identify exactly one write column",
                    file.file_name
                ),
                source: None,
            })?;
        let stored = if file.schema_id == table.schema().id() {
            None
        } else {
            Some(table.schema_manager().schema(file.schema_id).await?)
        };
        let fields = stored
            .as_ref()
            .map_or(table.schema().fields(), |schema| schema.fields());
        ids.extend(
            super::super::data_evolution_fields::project_by_paths(fields, columns)?
                .iter()
                .map(DataField::id),
        );
    }
    Ok(ids)
}

/// Only updated payloads enter the Arrow array. Unchanged values are NULL in
/// memory and receive a distinct placeholder tag when a baseline exists.
pub(super) fn delta_batch(
    field: &DataField,
    batches: &[RecordBatch],
    matches: &[(usize, usize, usize)],
    row_count: usize,
    has_baseline: bool,
) -> Result<(RecordBatch, Option<Vec<usize>>)> {
    let length = if has_baseline {
        matches.last().map_or(0, |(offset, _, _)| offset + 1)
    } else {
        row_count
    };
    let target = update_arrow_type(field, batches)?;
    let mut arrays = vec![arrow_array::new_null_array(&target, 1)];
    for batch in batches {
        arrays.push(super::super::update_input::cast_update_value(
            &matched_column(batch, field.name())?,
            &target,
            super::super::update_input::CastMode::RowUpdate,
        )?);
    }
    let mut indices = vec![(0, 0); length];
    for &(offset, batch, row) in matches {
        if offset >= row_count || offset >= length {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Blob update position {offset} exceeds logical row count {row_count}"
                ),
                source: None,
            });
        }
        if !field.data_type().is_nullable() && arrays[batch + 1].is_null(row) {
            return Err(crate::Error::DataInvalid {
                message: format!("Cannot update non-nullable Blob '{}' to NULL", field.name()),
                source: None,
            });
        }
        indices[offset] = (batch + 1, row);
    }
    if !has_baseline && !field.data_type().is_nullable() && matches.len() != row_count {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Non-nullable Blob '{}' has no baseline for unchanged rows",
                field.name()
            ),
            source: None,
        });
    }
    let values = interleave(
        &arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>(),
        &indices,
    )
    .map_err(|error| crate::Error::DataInvalid {
        message: format!(
            "Failed to assemble Blob update for '{}': {error}",
            field.name()
        ),
        source: Some(Box::new(error)),
    })?;
    // Unchanged positions are NULL in Arrow and placeholders on disk, even
    // for a non-nullable logical Blob column.
    let schema = crate::arrow::build_target_arrow_schema(std::slice::from_ref(field))?;
    let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        vec![schema
            .field(0)
            .clone()
            .with_data_type(target)
            .with_nullable(true)],
        schema.metadata().clone(),
    ));
    let batch =
        RecordBatch::try_new(schema, vec![values]).map_err(|error| crate::Error::DataInvalid {
            message: format!("Failed to build Blob update batch: {error}"),
            source: Some(Box::new(error)),
        })?;
    Ok((
        batch,
        has_baseline.then(|| matches.iter().map(|(offset, _, _)| *offset).collect()),
    ))
}

/// Preserve Java's nullable map keys through a list of key/value structs.
/// This representation is used only by the row-aware Blob update transport.
fn update_arrow_type(field: &DataField, batches: &[RecordBatch]) -> Result<arrow_schema::DataType> {
    use arrow_schema::DataType;
    let target = crate::arrow::paimon_type_to_arrow(field.data_type())?;
    let DataType::Map(_, _) = &target else {
        return Ok(target);
    };
    let list_input = batches
        .iter()
        .map(|batch| matched_column(batch, field.name()))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|array| {
            matches!(array.data_type(), DataType::List(entry)
            if matches!(entry.data_type(), DataType::Struct(fields) if fields.len() == 2))
        });
    if !list_input {
        return Ok(target);
    }
    Ok(
        super::super::write_batch_normalize::blob_map_row_type(&target)
            .expect("logical Blob maps have key/value entries"),
    )
}
