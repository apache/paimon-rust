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

use crate::io::OutputFile;
use crate::spec::{extract_datum_from_arrow, CoreOptions, DataField};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::bitmap_global_index_writer::write_sorted_bitmap_file;
use crate::table::centroid_index::{self, Descriptor, Role};
use crate::table::index_build::{
    fields::validate_btree_field,
    sort::{IndexSorter, SortedRows},
    vector::temporary_vector_file,
    IndexPayload,
};
use crate::table::sorted_global_index_options::SortedIndexWriteOptions;
use crate::table::Table;
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

pub(super) const EXTRA_FIELDS: &str = "extra_fields";

#[derive(Clone)]
pub(super) struct BitmapBuild {
    fields: Vec<DataField>,
    options: SortedIndexWriteOptions,
    sort_budget: usize,
}

impl BitmapBuild {
    pub(super) fn resolve(
        table: &Table,
        vector: &str,
        options: &HashMap<String, String>,
    ) -> Result<Option<Self>> {
        let Some(json) = options.get(EXTRA_FIELDS) else {
            return Ok(None);
        };
        let columns: BTreeMap<String, String> = serde_json::from_str(json).map_err(|error| {
            centroid_index::invalid(format!(
                "'extra_fields' must be a JSON object mapping columns to 'bitmap': {error}"
            ))
        })?;
        if columns.is_empty() {
            return Ok(None);
        }
        let fields = table.schema().fields();
        let fields = columns
            .into_iter()
            .map(|(column, kind)| {
                if column == vector || !kind.eq_ignore_ascii_case("bitmap") {
                    return Err(centroid_index::invalid(format!(
                        "Invalid centroid bitmap field '{column}' with index type '{kind}'"
                    )));
                }
                let field = fields
                    .iter()
                    .find(|field| field.name() == column)
                    .ok_or_else(|| {
                        centroid_index::invalid(format!("Unknown centroid bitmap field '{column}'"))
                    })?;
                validate_btree_field(field)?;
                Ok(field.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_budget = CoreOptions::new(options).write_buffer_size()? / fields.len();
        Ok(Some(Self {
            fields,
            options: SortedIndexWriteOptions::from_options("bitmap", options)?,
            sort_budget,
        }))
    }

    pub(super) fn fields(&self) -> impl Iterator<Item = &DataField> {
        self.fields.iter()
    }

    pub(super) fn sorters(&self) -> Vec<IndexSorter> {
        self.fields()
            .map(|field| {
                IndexSorter::new(
                    self.sort_budget,
                    make_bitmap_key_comparator(field.data_type()),
                )
            })
            .collect()
    }

    pub(super) async fn write(
        &self,
        output: OutputFile,
        rows: SortedRows,
        row_count: i64,
        field_id: i32,
    ) -> Result<IndexPayload> {
        let mut writer = output.writer().await?;
        let result = write_sorted_bitmap_file(writer.as_mut(), rows, &self.options).await?;
        if result.row_count != row_count as u64 {
            return Err(centroid_index::invalid(
                "Centroid Bitmap row count mismatch",
            ));
        }
        writer.close().await?;
        Ok(IndexPayload {
            row_count,
            index_meta: Descriptor {
                role: Role::Bitmap(field_id as usize),
                payload: Some(&result.meta.serialize()),
            }
            .encode()?,
        })
    }
}

/// The offset file maps relative row IDs to packed scalar keys. Rows without
/// vectors have zero offsets, and whole batches without vectors leave holes.
pub(super) struct BitmapSpill {
    offsets: BufWriter<tokio::fs::File>,
    keys: BufWriter<tokio::fs::File>,
    rows: u64,
    next_offset_row: u64,
    key_bytes: u64,
}

const NULL_KEY: u32 = u32::MAX;

impl BitmapSpill {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            offsets: BufWriter::new(temporary_vector_file("centroid Bitmap offsets")?),
            keys: BufWriter::new(temporary_vector_file("centroid Bitmap keys")?),
            rows: 0,
            next_offset_row: 0,
            key_bytes: 0,
        })
    }

    pub(super) async fn record(
        &mut self,
        bitmap: &BitmapBuild,
        batch: &RecordBatch,
        vector: &str,
    ) -> Result<()> {
        let vectors = batch.column_by_name(vector).ok_or_else(|| {
            centroid_index::invalid("Vector column missing from centroid Bitmap batch")
        })?;
        let start = self.rows;
        self.rows += batch.num_rows() as u64;
        if vectors.null_count() == batch.num_rows() {
            return Ok(());
        }
        let schema = batch.schema();
        let indices = bitmap
            .fields()
            .map(|field| {
                schema
                    .index_of(field.name())
                    .map_err(|error| centroid_index::invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut offsets = vec![0; batch.num_rows() * size_of::<u64>()];
        for row in 0..batch.num_rows() {
            if vectors.is_null(row) {
                continue;
            }
            // Offset zero identifies a hole, so store offsets starting at one.
            let slot = row * size_of::<u64>();
            offsets[slot..slot + size_of::<u64>()]
                .copy_from_slice(&(self.key_bytes + 1).to_le_bytes());
            for (field, &index) in bitmap.fields().zip(&indices) {
                let key = extract_datum_from_arrow(batch, row, index, field.data_type())?
                    .map(|datum| serialize_bitmap_datum(&datum, field.data_type()));
                let len = match &key {
                    Some(key) => u32::try_from(key.len())
                        .ok()
                        .filter(|&len| len != NULL_KEY)
                        .ok_or_else(|| {
                            centroid_index::invalid("Centroid Bitmap key is too large")
                        })?,
                    None => NULL_KEY,
                };
                self.keys.write_u32_le(len).await.map_err(spill_error)?;
                self.key_bytes += size_of::<u32>() as u64;
                if let Some(key) = key {
                    self.keys.write_all(&key).await.map_err(spill_error)?;
                    self.key_bytes += u64::from(len);
                }
            }
        }
        if start != self.next_offset_row {
            self.offsets
                .seek(SeekFrom::Start(offset_slot(start)?))
                .await
                .map_err(spill_error)?;
        }
        self.offsets
            .write_all(&offsets)
            .await
            .map_err(spill_error)?;
        self.next_offset_row = self.rows;
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<BitmapSpillReader> {
        self.offsets.flush().await.map_err(spill_error)?;
        self.keys.flush().await.map_err(spill_error)?;
        Ok(BitmapSpillReader {
            offsets: BufReader::new(self.offsets.into_inner().into_std().await),
            keys: BufReader::new(self.keys.into_inner().into_std().await),
        })
    }
}

pub(super) struct BitmapSpillReader {
    offsets: BufReader<std::fs::File>,
    keys: BufReader<std::fs::File>,
}

impl BitmapSpillReader {
    /// Both the routed IDs and the spilled offsets follow source row order.
    /// Relative seeks retain buffered reads for nearby rows.
    pub(super) fn add_rows(
        &mut self,
        sorters: &mut [IndexSorter],
        row_ids: impl Iterator<Item = Result<i64>>,
        row_id_base: i64,
    ) -> Result<()> {
        self.offsets.rewind().map_err(spill_error)?;
        self.keys.rewind().map_err(spill_error)?;
        let mut offsets_pos = 0;
        let mut keys_pos = 0;
        for row_id in row_ids {
            let row_id = row_id?;
            let bitmap_row_id = row_id_base
                .checked_add(row_id)
                .ok_or_else(|| centroid_index::invalid("Centroid Bitmap position overflow"))?;
            let offset =
                offset_slot(u64::try_from(row_id).map_err(crate::table::index_build::error)?)?;
            seek_relative(&mut self.offsets, offsets_pos, offset)?;
            let mut bytes = [0; size_of::<u64>()];
            self.offsets.read_exact(&mut bytes).map_err(spill_error)?;
            offsets_pos = offset + bytes.len() as u64;
            let key_offset = u64::from_le_bytes(bytes)
                .checked_sub(1)
                .ok_or_else(|| centroid_index::invalid("Missing scalar key for a centroid row"))?;
            seek_relative(&mut self.keys, keys_pos, key_offset)?;
            keys_pos = key_offset;
            for sorter in &mut *sorters {
                let mut bytes = [0; size_of::<u32>()];
                self.keys.read_exact(&mut bytes).map_err(spill_error)?;
                keys_pos += bytes.len() as u64;
                let len = u32::from_le_bytes(bytes);
                let key = if len == NULL_KEY {
                    None
                } else {
                    let mut key = vec![0; len as usize];
                    self.keys.read_exact(&mut key).map_err(spill_error)?;
                    keys_pos += u64::from(len);
                    Some(key)
                };
                sorter.push((key, bitmap_row_id))?;
            }
        }
        Ok(())
    }
}

fn offset_slot(row_id: u64) -> Result<u64> {
    row_id
        .checked_mul(size_of::<u64>() as u64)
        .ok_or_else(|| centroid_index::invalid("Centroid Bitmap spill offset overflow"))
}

fn seek_relative<R: Read + Seek>(reader: &mut BufReader<R>, from: u64, to: u64) -> Result<()> {
    let from = i64::try_from(from).map_err(crate::table::index_build::error)?;
    let to = i64::try_from(to).map_err(crate::table::index_build::error)?;
    reader.seek_relative(to - from).map_err(spill_error)
}

fn spill_error(error: io::Error) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Centroid Bitmap temporary file: {error}"),
        source: Some(Box::new(error)),
    }
}
