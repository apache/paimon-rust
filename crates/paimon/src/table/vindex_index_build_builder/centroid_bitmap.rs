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
use crate::spec::{extract_datum_from_arrow, DataField};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::bitmap_global_index_writer::BitmapIndexBuilder;
use crate::table::centroid_bitmap::add_section;
use crate::table::centroid_index::{self, Descriptor, Role};
use crate::table::index_build::{fields::validate_btree_field, IndexPayload};
use crate::table::sorted_global_index_options::SortedIndexWriteOptions;
use crate::table::Table;
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::{BTreeMap, HashMap};

pub(super) const EXTRA_FIELDS: &str = "extra_fields";

pub(super) struct BitmapBuild {
    columns: Vec<BitmapColumn>,
    options: SortedIndexWriteOptions,
    schema_id: i64,
}

struct BitmapColumn {
    field: DataField,
    keys: Vec<Option<Vec<u8>>>,
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
        let columns = columns
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
                Ok(BitmapColumn {
                    field: field.clone(),
                    keys: vec![],
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self {
            columns,
            options: SortedIndexWriteOptions::from_options("bitmap", options)?,
            schema_id: table.schema().id(),
        }))
    }

    pub(super) fn fields(&self) -> impl Iterator<Item = &DataField> {
        self.columns.iter().map(|column| &column.field)
    }

    pub(super) fn record(&mut self, batch: &RecordBatch) -> Result<()> {
        for column in &mut self.columns {
            let field = &column.field;
            let index = batch
                .schema()
                .index_of(field.name())
                .map_err(|error| centroid_index::invalid(error.to_string()))?;
            for row in 0..batch.num_rows() {
                let key = extract_datum_from_arrow(batch, row, index, field.data_type())?
                    .map(|datum| serialize_bitmap_datum(&datum, field.data_type()));
                column.keys.push(key);
            }
        }
        Ok(())
    }

    pub(super) async fn write(
        &self,
        output: OutputFile,
        local_row_ids: &[i64],
        model_id: &str,
        centroid: usize,
    ) -> Result<IndexPayload> {
        let mut writer = output.writer().await?;
        let mut directory = self.schema_id.to_be_bytes().to_vec();
        for column in &self.columns {
            let mut bitmap = BitmapIndexBuilder::new(
                self.options.block_size,
                self.options.compression_type,
                self.options.compression_level,
                make_bitmap_key_comparator(column.field.data_type()),
            );
            for &row_id in local_row_ids {
                let key = usize::try_from(row_id)
                    .ok()
                    .and_then(|row| column.keys.get(row))
                    .ok_or_else(|| {
                        centroid_index::invalid("Missing scalar key for a centroid row")
                    })?;
                bitmap
                    .write(key.as_deref(), row_id)
                    .map_err(crate::table::index_build::error)?;
            }
            let (bytes, result) = bitmap.encode().map_err(crate::table::index_build::error)?;
            add_section(&mut directory, bytes.len(), &result.meta.serialize())?;
            writer.write(bytes).await?;
        }
        writer.close().await?;
        Ok(IndexPayload {
            row_count: local_row_ids.len() as i64,
            index_meta: Descriptor {
                model_id,
                version: 2,
                role: Role::Bitmap(centroid),
                payload: Some(&directory),
            }
            .encode()?,
        })
    }
}
