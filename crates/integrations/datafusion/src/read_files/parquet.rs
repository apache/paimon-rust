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

use super::File;
use crate::error::to_datafusion_error;
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::physical_plan::parquet::{
    metadata::DFParquetMetadata, transform_schema_to_view,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::object_store::ObjectStore;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::schema::types::ColumnDescPtr;
use futures::{stream, StreamExt, TryStreamExt};
use paimon::arrow::{arrow_field_to_paimon_type, paimon_type_to_arrow_field};
use paimon::spec::{DataType as PaimonType, GeographyType, GeometryType};
use std::sync::Arc;

pub(super) async fn infer_schema(
    state: &SessionState,
    store: &Arc<dyn ObjectStore>,
    files: &[File],
    format: &ParquetFormat,
) -> Result<SchemaRef> {
    let schemas = stream::iter(files.iter().map(|file| file.meta.clone()))
        .map(|meta| async move {
            let metadata = DFParquetMetadata::new(store.as_ref(), &meta)
                .with_metadata_size_hint(format.metadata_size_hint())
                .fetch_metadata()
                .await?;
            let file = metadata.file_metadata();
            let descriptor = file.schema_descr();
            let schema = parquet_to_arrow_schema(descriptor, file.key_value_metadata())?;
            let mut columns = descriptor.columns().iter();
            let fields = schema
                .fields()
                .iter()
                .map(|field| geo_field(field, &mut columns, true))
                .collect::<paimon::Result<Fields>>()
                .map_err(to_datafusion_error)?;
            Ok::<_, DataFusionError>(Schema::new(fields))
        })
        .boxed()
        .buffered(
            state
                .config_options()
                .execution
                .meta_fetch_concurrency
                .get(),
        )
        .try_collect::<Vec<_>>()
        .await?;
    let schema = Schema::try_merge(schemas)?;
    Ok(Arc::new(transform_schema_to_view(&schema)))
}

// Match Arrow and Parquet leaves in depth-first order, bypassing Parquet's LIST/MAP wrappers.
fn geo_field<'a>(
    field: &FieldRef,
    columns: &mut impl Iterator<Item = &'a ColumnDescPtr>,
    top_level: bool,
) -> paimon::Result<FieldRef> {
    let mut result = Arc::clone(field);
    // DataFusion drops top-level metadata before merging files. Restore only the geo type.
    if top_level && !field.metadata().is_empty() {
        Arc::make_mut(&mut result).metadata_mut().clear();
    }
    let mut child = |f: &FieldRef| geo_field(f, columns, false);
    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(child).collect::<paimon::Result<_>>()?)
        }
        DataType::List(f) => DataType::List(child(f)?),
        DataType::LargeList(f) => DataType::LargeList(child(f)?),
        DataType::FixedSizeList(f, len) => DataType::FixedSizeList(child(f)?, *len),
        DataType::ListView(f) => DataType::ListView(child(f)?),
        DataType::LargeListView(f) => DataType::LargeListView(child(f)?),
        DataType::Map(f, sorted) => DataType::Map(child(f)?, *sorted),
        _ => {
            let logical = columns.next().and_then(|c| c.logical_type_ref());
            let ty = match logical {
                Some(LogicalType::Geometry(g)) => PaimonType::Geometry(GeometryType::new(
                    g.crs.as_deref().unwrap_or(GeometryType::DEFAULT_CRS),
                )?),
                Some(LogicalType::Geography(g)) => PaimonType::Geography(GeographyType::new(
                    g.crs.as_deref().unwrap_or(GeographyType::DEFAULT_CRS),
                    g.algorithm.unwrap_or_default().to_string().parse()?,
                )?),
                _ if field.extension_type_name() == Some("geoarrow.wkb") => {
                    arrow_field_to_paimon_type(field)?
                }
                _ => return Ok(result),
            };
            let geo = paimon_type_to_arrow_field(field.name(), &ty)?;
            // Native WKB and embedded Binary/LargeBinary/BinaryView must merge at any depth.
            let result_field = Arc::make_mut(&mut result);
            result_field.set_data_type(DataType::BinaryView);
            result_field.metadata_mut().extend(geo.metadata().clone());
            return Ok(result);
        }
    };
    Arc::make_mut(&mut result).set_data_type(data_type);
    Ok(result)
}
