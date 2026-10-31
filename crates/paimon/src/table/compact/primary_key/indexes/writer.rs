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

//! Native writers shared with global indexes, fed physical PK file ordinals.

use super::super::super::invalid;
use crate::resource::ResourceContext;
use crate::spec::{
    extract_datum_from_arrow, DataFileMeta, DataType, Datum, GlobalIndexMeta, IndexFileMeta,
    ROW_ID_FIELD_NAME,
};
use crate::table::global_index_build_common::copy_local_file_to_output;
use crate::table::index_file_path::IndexFileLocation;
use crate::table::pk_index_options::Definition;
use crate::table::sorted_global_index_build_builder::{
    extraction::{emit_multivalue_rows, emit_scalar_rows},
    make_index_key_codec,
    sort::IndexSorter,
    validation::index_key_type,
};
use crate::table::sorted_global_index_options::SortedIndexWriteOptions;
use crate::table::vindex_index_build_builder::{
    extraction::validate_vector_batch, writer::train_and_add_spilled_vectors,
};
use crate::table::{DataSplit, Table};
use crate::{Error, Result};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use futures::TryStreamExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

fn native_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::DataInvalid {
        message: "PK index build failed".into(),
        source: Some(Box::new(error)),
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(native_error)?
}

pub(super) fn native_options(
    table: &Table,
    definition: &Definition,
) -> Result<BTreeMap<String, String>> {
    if crate::vindex::is_vindex_index_type(&definition.index_type) {
        return Ok(crate::vindex::VindexVectorIndexOptions::new(
            table.schema().options(),
            &definition.options,
            &definition.index_type,
            &definition.field,
        )?
        .native_options
        .into_iter()
        .collect());
    }
    let mut options = table.schema().options().clone();
    options.extend(definition.options.clone());
    Ok(options
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("full-text.")
                .map(|key| (key.to_string(), value))
        })
        .collect())
}

/// Keep every physical row, including tombstones, NULLs, and DV-hidden rows.
/// Native extractors use a synthetic consecutive row ID local to this level.
async fn batches<S, F, Fut>(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    resources: Option<ResourceContext>,
    mut state: S,
    mut consume: F,
) -> Result<S>
where
    F: FnMut(S, RecordBatch, i64) -> Fut,
    Fut: Future<Output = Result<S>>,
{
    let mut offset = 0i64;
    for file in files {
        let reader = super::super::new_reader(table, resources.clone())?;
        let mut stream = reader.read_physical_file(split.clone(), file.clone())?;
        while let Some(batch) = stream.try_next().await? {
            let count = batch.num_rows() as i64;
            let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
            fields.push(Arc::new(Field::new(
                ROW_ID_FIELD_NAME,
                ArrowType::Int64,
                false,
            )));
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(Int64Array::from_iter_values(
                offset..offset + count,
            )));
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(native_error)?;
            state = consume(state, batch, offset).await?;
            offset = offset
                .checked_add(count)
                .ok_or_else(|| invalid("PK index ordinal overflow"))?;
        }
    }
    if offset != super::row_count(files)? {
        return Err(invalid("PK index source row count changed"));
    }
    Ok(state)
}

pub(super) async fn build(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    definition: &Definition,
    resources: Option<ResourceContext>,
) -> Result<Option<IndexFileMeta>> {
    let kind = definition.index_type.as_str();
    let count = super::row_count(files)?;
    let file_name = format!("{kind}-pk-index-{}.index", uuid::Uuid::new_v4());
    let in_data = table.schema().core_options().index_file_in_data_file_dir();
    let relative = split
        .bucket_path()
        .strip_prefix(&table.data_file_location())
        .unwrap_or(split.bucket_path())
        .trim_start_matches('/');
    let external_path = crate::table::external_path::new_index_external_path(
        table.schema().options(),
        in_data,
        relative,
        &file_name,
    )?;
    let location = IndexFileLocation::BucketLocal {
        table_path: table.location(),
        bucket_path: split.bucket_path(),
        index_file_in_data_file_dir: in_data,
    };
    let path = location.resolve(&file_name, external_path.as_deref());
    if let Some((directory, _)) = path.rsplit_once('/') {
        table.file_io().mkdirs(directory).await?;
    }
    let mut options = table.schema().options().clone();
    options.extend(definition.options.clone());
    let written: Result<Option<Vec<u8>>> = async {
        if crate::vindex::is_vindex_index_type(kind) {
            vector(table,split,files,definition,resources,&path).await
        } else if kind == "full-text" {
            full_text(table,split,files,definition,resources,&path).await.map(Some)
        } else {
            let field = definition.field.clone();
            let codec_type = if kind == "btree" { "btree" } else { "bitmap" };
            let key_type = index_key_type(kind,&field)?;
            let (cmp,serialize) = make_index_key_codec(codec_type,key_type);
            let output = table.file_io().new_output(&path)?.writer().await?;
            if kind == "fm" {
                let writer = crate::fm_index::FMGlobalIndexWriter::new(output,crate::fm_index::FMOptions::from_options(&options)?.write).map_err(native_error)?;
                let writer = batches(table,split,files,resources,writer,|mut native,batch,offset| {
                    let field = field.clone();
                    async move {
                        let index = batch.schema().index_of(field.name()).map_err(native_error)?;
                        for row in 0..batch.num_rows() {
                            let key = extract_datum_from_arrow(&batch,row,index,field.data_type())?.map(|d| serialize(&d,field.data_type()));
                            native.write(key.as_deref(),(offset+row as i64) as u64).await.map_err(native_error)?;
                        }
                        Ok(native)
                    }
                }).await?;
                return Ok(Some(writer.finish().await.map_err(native_error)?.index_meta));
            }
            let sorter = IndexSorter::new(crate::spec::CoreOptions::new(&options).write_buffer_size()?,cmp);
            let sorter = batches(table,split,files,resources,sorter,|mut native,batch,offset| {
                let field = field.clone(); let kind = kind.to_string();
                async move {
                    blocking(move || {
                        let mut emit = |row| native.push(row);
                        if kind == "multivalue" {
                            let DataType::Array(array) = field.data_type() else { unreachable!() };
                            emit_multivalue_rows(std::slice::from_ref(&batch),field.name(),array.element_type(),0,batch.num_rows() as i64,serialize,offset,&mut emit)?;
                        } else {
                            emit_scalar_rows(std::slice::from_ref(&batch),field.name(),field.data_type(),0,batch.num_rows() as i64,serialize,offset,&mut emit)?;
                        }
                        Ok(native)
                    }).await
                }
            }).await?;
            let mut rows = blocking(move || sorter.finish()).await?;
            let options = SortedIndexWriteOptions::from_options(kind,&options)?;
            let cmp = make_index_key_codec(codec_type,key_type).0;
            if kind == "btree" {
                let mut writer = crate::btree::BTreeIndexWriter::with_comparator_and_options(output,options.block_size,options.compression_type,options.compression_level,options.bloom_filter_enabled,cmp)
                    .with_file_version(options.btree_file_version).map_err(native_error)?;
                while let Some((key,id)) = rows.next().await? { writer.write(key.as_deref(),id).await.map_err(native_error)?; }
                Ok(Some(writer.finish().await.map_err(native_error)?.meta.serialize()))
            } else {
                let mut writer = crate::table::bitmap_global_index_writer::BitmapGlobalIndexWriter::with_compression_level(output,options.block_size,options.compression_type,options.compression_level,cmp);
                while let Some((key,id)) = rows.next().await? {
                    if kind == "multivalue" { writer.write_posting(key.as_deref().unwrap(),id).map_err(native_error)?; }
                    else { writer.write(key.as_deref(),id).map_err(native_error)?; }
                }
                let result = if kind == "multivalue" { writer.finish_with_source_row_count(count as u64).await }
                    else { writer.finish().await }.map_err(native_error)?;
                Ok(Some(result.meta.serialize()))
            }
        }
    }.await;
    let index_meta = match written {
        Ok(Some(meta)) => meta,
        Ok(None) => return Ok(None),
        Err(error) => {
            let _ = table.file_io().delete_file(&path).await;
            return Err(error);
        }
    };
    let metadata = async {
        let file_size =
            i64::try_from(table.file_io().get_status(&path).await?.size).map_err(native_error)?;
        Ok(Some(IndexFileMeta {
            index_type: kind.to_string(),
            file_name,
            file_size,
            row_count: count,
            deletion_vectors_ranges: None,
            external_path,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: count - 1,
                index_field_id: definition.field.id(),
                extra_field_ids: None,
                source_meta: Some(super::source_meta(files[0].level, files)?.serialize()?),
                index_meta: Some(index_meta),
            }),
        }))
    }
    .await;
    if metadata.is_err() {
        let _ = table.file_io().delete_file(&path).await;
    }
    metadata
}

async fn full_text(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    definition: &Definition,
    resources: Option<ResourceContext>,
    path: &str,
) -> Result<Vec<u8>> {
    let native = native_options(table, definition)?;
    let config = paimon_ftindex_core::FullTextIndexConfig::from_options(
        &native.clone().into_iter().collect(),
    )
    .map_err(native_error)?;
    let writer = blocking(move || {
        paimon_ftindex_core::FullTextIndexWriter::new(config).map_err(native_error)
    })
    .await?;
    let mut writer = batches(
        table,
        split,
        files,
        resources,
        writer,
        |mut native, batch, offset| {
            let field = definition.field.clone();
            async move {
                blocking(move || {
                    let column = batch
                        .schema()
                        .index_of(field.name())
                        .map_err(native_error)?;
                    for row in 0..batch.num_rows() {
                        if let Some(Datum::String(text)) =
                            extract_datum_from_arrow(&batch, row, column, field.data_type())?
                        {
                            native
                                .add_document(offset + row as i64, text)
                                .map_err(native_error)?;
                        }
                    }
                    Ok(native)
                })
                .await
            }
        },
    )
    .await?;
    let file = blocking(move || {
        let mut file = tempfile::NamedTempFile::new().map_err(native_error)?;
        writer
            .write(&mut paimon_ftindex_core::io::PosWriter::new(
                file.as_file_mut(),
            ))
            .map_err(native_error)?;
        Ok(file)
    })
    .await?;
    copy_local_file_to_output(file.path(), table.file_io().new_output(path)?).await?;
    serde_json::to_vec(&native).map_err(native_error)
}

async fn vector(
    table: &Table,
    split: &DataSplit,
    files: &[DataFileMeta],
    definition: &Definition,
    resources: Option<ResourceContext>,
    path: &str,
) -> Result<Option<Vec<u8>>> {
    let options = crate::vindex::VindexVectorIndexOptions::new(
        table.schema().options(),
        &definition.options,
        &definition.index_type,
        &definition.field,
    )?;
    let dimension = options.dimension();
    let raw = tokio::io::BufWriter::new(tokio::fs::File::from_std(
        tempfile::tempfile().map_err(native_error)?,
    ));
    let ids = tokio::io::BufWriter::new(tokio::fs::File::from_std(
        tempfile::tempfile().map_err(native_error)?,
    ));
    let (mut raw, mut ids, count) = batches(
        table,
        split,
        files,
        resources,
        (raw, ids, 0usize),
        |(mut raw, mut ids, count), batch, offset| {
            let column = definition.field.name();
            async move {
                let mut expected = offset;
                let vectors = validate_vector_batch(&batch, column, dimension, &mut expected)?;
                raw.write_all(vectors.values.inner().as_slice())
                    .await
                    .map_err(native_error)?;
                ids.write_all(vectors.row_ids.inner().as_slice())
                    .await
                    .map_err(native_error)?;
                Ok((raw, ids, count + vectors.vector_count))
            }
        },
    )
    .await?;
    if count == 0 {
        return Ok(None);
    }
    raw.flush().await.map_err(native_error)?;
    ids.flush().await.map_err(native_error)?;
    let raw = raw.into_inner().into_std().await;
    let ids = ids.into_inner().into_std().await;
    let config = options.training_config(count)?;
    let ratio = options.train_sample_ratio;
    let file = blocking(move || {
        let (mut writer, _, _, _) =
            train_and_add_spilled_vectors(raw, ids, config, count, dimension, ratio, false)
                .map_err(native_error)?;
        let mut file = tempfile::NamedTempFile::new().map_err(native_error)?;
        writer
            .write(&mut paimon_vindex_core::io::PosWriter::new(
                file.as_file_mut(),
            ))
            .map_err(native_error)?;
        Ok(file)
    })
    .await?;
    copy_local_file_to_output(file.path(), table.file_io().new_output(path)?).await?;
    serde_json::to_vec(&native_options(table, definition)?)
        .map(Some)
        .map_err(native_error)
}
