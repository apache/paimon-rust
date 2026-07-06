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

use crate::arrow::{
    arrow_to_paimon_type, build_target_arrow_schema, is_variant_arrow_fields, paimon_type_to_arrow,
};
use crate::spec::{
    is_variant_extraction_row, parse_variant_metadata, ArrayType, DataField, DataType, RowType,
};
use crate::variant::{
    build_variant_schema, cast_shredded, cast_variant_to_shredded_value,
    infer_variant_shredding_schema, rebuild_shredded, variant_shredding_type, GenericVariant,
    ShreddedRow, ShreddedValue, VariantShreddingInferConfig, VARIANT_METADATA_FIELD_NAME,
    VARIANT_TYPED_VALUE_FIELD_NAME, VARIANT_VALUE_FIELD_NAME,
};
use crate::{Error, Result};
use arrow_array::{
    new_null_array, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

const VARIANT_SHREDDING_SCHEMA_OPTION: &str = "variant.shreddingSchema";
const PARQUET_VARIANT_SHREDDING_SCHEMA_OPTION: &str = "parquet.variant.shreddingSchema";
const VARIANT_INFER_SHREDDING_SCHEMA_OPTION: &str = "variant.inferShreddingSchema";
const PARQUET_VARIANT_INFER_SHREDDING_SCHEMA_OPTION: &str = "parquet.variant.inferShreddingSchema";
const VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW_OPTION: &str = "variant.shredding.maxInferBufferRow";
const VARIANT_SHREDDING_MAX_SCHEMA_DEPTH_OPTION: &str = "variant.shredding.maxSchemaDepth";
const VARIANT_SHREDDING_MAX_SCHEMA_WIDTH_OPTION: &str = "variant.shredding.maxSchemaWidth";
const VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO_OPTION: &str =
    "variant.shredding.minFieldCardinalityRatio";
const DEFAULT_VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW: usize = 4096;
const DEFAULT_VARIANT_SHREDDING_MAX_SCHEMA_DEPTH: usize = 50;
const DEFAULT_VARIANT_SHREDDING_MAX_SCHEMA_WIDTH: usize = 300;
const DEFAULT_VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO: f64 = 0.1;

pub(crate) fn configured_variant_shredding_fields(
    logical_fields: &[DataField],
    options: &HashMap<String, String>,
) -> Result<Option<Vec<DataField>>> {
    let Some(configured) = configured_shredding_schema(options)? else {
        return Ok(None);
    };
    let physical = physical_fields_for_configured_shredding(logical_fields, &configured)?;
    if physical == logical_fields {
        Ok(None)
    } else {
        Ok(Some(physical))
    }
}

pub(crate) fn should_infer_variant_shredding_fields(
    logical_fields: &[DataField],
    options: &HashMap<String, String>,
) -> Result<bool> {
    if has_configured_shredding_schema(options) {
        return Ok(false);
    }
    if !option_bool(
        options,
        &[
            VARIANT_INFER_SHREDDING_SCHEMA_OPTION,
            PARQUET_VARIANT_INFER_SHREDDING_SCHEMA_OPTION,
        ],
        false,
    )? {
        return Ok(false);
    }
    Ok(contains_variant_fields(logical_fields))
}

pub(crate) fn variant_shredding_infer_buffer_row_count(
    options: &HashMap<String, String>,
) -> Result<usize> {
    option_usize(
        options,
        VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW_OPTION,
        DEFAULT_VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW,
    )
}

pub(crate) fn infer_variant_shredding_fields(
    logical_fields: &[DataField],
    sample_batches: &[RecordBatch],
    options: &HashMap<String, String>,
) -> Result<Option<Vec<DataField>>> {
    let paths = paths_to_variant(logical_fields);
    if paths.is_empty() {
        return Ok(None);
    }

    let config = VariantShreddingInferConfig {
        max_schema_depth: option_usize(
            options,
            VARIANT_SHREDDING_MAX_SCHEMA_DEPTH_OPTION,
            DEFAULT_VARIANT_SHREDDING_MAX_SCHEMA_DEPTH,
        )?,
        min_field_cardinality_ratio: option_f64(
            options,
            VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO_OPTION,
            DEFAULT_VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO,
        )?,
    };
    let mut max_fields_remaining = option_usize(
        options,
        VARIANT_SHREDDING_MAX_SCHEMA_WIDTH_OPTION,
        DEFAULT_VARIANT_SHREDDING_MAX_SCHEMA_WIDTH,
    )?;

    let row_type = RowType::new(logical_fields.to_vec());
    let mut inferred_types = HashMap::new();
    for path in paths {
        let mut variants = Vec::new();
        for batch in sample_batches {
            for row in 0..batch.num_rows() {
                if let Some(variant) = variant_at_path(batch, &row_type, &path, row)? {
                    variants.push(variant);
                }
            }
        }

        let inferred =
            infer_variant_shredding_schema(&variants, &config, &mut max_fields_remaining)?;
        if !matches!(inferred, DataType::Variant(_)) {
            inferred_types.insert(path, variant_shredding_type(&inferred)?);
        }
    }

    let physical = update_inferred_variant_schema(&row_type, &inferred_types, &[])?;
    let physical_fields = physical.fields().to_vec();
    if physical_fields == logical_fields {
        Ok(None)
    } else {
        Ok(Some(physical_fields))
    }
}

fn has_configured_shredding_schema(options: &HashMap<String, String>) -> bool {
    options.contains_key(VARIANT_SHREDDING_SCHEMA_OPTION)
        || options.contains_key(PARQUET_VARIANT_SHREDDING_SCHEMA_OPTION)
}

fn configured_shredding_schema(options: &HashMap<String, String>) -> Result<Option<RowType>> {
    let Some(raw) = options
        .get(VARIANT_SHREDDING_SCHEMA_OPTION)
        .or_else(|| options.get(PARQUET_VARIANT_SHREDDING_SCHEMA_OPTION))
    else {
        return Ok(None);
    };
    let mut value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| Error::DataInvalid {
            message: format!("Invalid variant shredding schema JSON: {e}"),
            source: Some(Box::new(e)),
        })?;
    add_missing_field_ids(&mut value);
    let data_type: DataType = serde_json::from_value(value).map_err(|e| Error::DataInvalid {
        message: format!("Invalid variant shredding schema JSON: {e}"),
        source: Some(Box::new(e)),
    })?;
    match data_type {
        DataType::Row(row) => Ok(Some(row)),
        other => Err(Error::DataInvalid {
            message: format!("Invalid variant shredding schema: expected ROW, got {other:?}"),
            source: None,
        }),
    }
}

fn add_missing_field_ids(value: &mut serde_json::Value) {
    let serde_json::Value::Object(map) = value else {
        return;
    };

    if let Some(serde_json::Value::Array(fields)) = map.get_mut("fields") {
        for (idx, field) in fields.iter_mut().enumerate() {
            let serde_json::Value::Object(field_map) = field else {
                continue;
            };
            field_map
                .entry("id")
                .or_insert_with(|| serde_json::Value::Number((idx as i64).into()));
            if let Some(field_type) = field_map.get_mut("type") {
                add_missing_field_ids(field_type);
            }
        }
    }
    if let Some(element) = map.get_mut("element") {
        add_missing_field_ids(element);
    }
    if let Some(key) = map.get_mut("key") {
        add_missing_field_ids(key);
    }
    if let Some(value) = map.get_mut("value") {
        add_missing_field_ids(value);
    }
}

fn option_bool(
    options: &HashMap<String, String>,
    keys: &[&str],
    default_value: bool,
) -> Result<bool> {
    let Some(value) = keys.iter().find_map(|key| options.get(*key)) else {
        return Ok(default_value);
    };
    value.parse::<bool>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid boolean option value '{value}'"),
        source: Some(Box::new(e)),
    })
}

fn option_usize(
    options: &HashMap<String, String>,
    key: &str,
    default_value: usize,
) -> Result<usize> {
    let Some(value) = options.get(key) else {
        return Ok(default_value);
    };
    value.parse::<usize>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid integer option {key}={value}"),
        source: Some(Box::new(e)),
    })
}

fn option_f64(options: &HashMap<String, String>, key: &str, default_value: f64) -> Result<f64> {
    let Some(value) = options.get(key) else {
        return Ok(default_value);
    };
    value.parse::<f64>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid double option {key}={value}"),
        source: Some(Box::new(e)),
    })
}

pub(crate) fn contains_variant_fields(fields: &[DataField]) -> bool {
    fields.iter().any(|field| match field.data_type() {
        DataType::Variant(_) => true,
        DataType::Row(row_type) => contains_variant_fields(row_type.fields()),
        _ => false,
    })
}

pub(crate) fn contains_variant_read_fields(fields: &[DataField]) -> bool {
    fields.iter().any(|field| match field.data_type() {
        DataType::Variant(_) => true,
        DataType::Row(row_type) if is_variant_extraction_row(row_type) => true,
        DataType::Row(row_type) => contains_variant_read_fields(row_type.fields()),
        _ => false,
    })
}

fn paths_to_variant(fields: &[DataField]) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    for (idx, field) in fields.iter().enumerate() {
        match field.data_type() {
            DataType::Variant(_) => result.push(vec![idx]),
            DataType::Row(row_type) => {
                for mut path in paths_to_variant(row_type.fields()) {
                    path.insert(0, idx);
                    result.push(path);
                }
            }
            _ => {}
        }
    }
    result
}

fn variant_at_path(
    batch: &RecordBatch,
    row_type: &RowType,
    path: &[usize],
    row: usize,
) -> Result<Option<GenericVariant>> {
    let Some((&field_idx, rest)) = path.split_first() else {
        return Ok(None);
    };
    let Some(field) = row_type.fields().get(field_idx) else {
        return Err(Error::DataInvalid {
            message: "Invalid Variant inference path".to_string(),
            source: None,
        });
    };
    let Some(array) = batch.columns().get(field_idx) else {
        return Err(Error::DataInvalid {
            message: "Variant inference path is outside RecordBatch".to_string(),
            source: None,
        });
    };
    variant_at_path_in_array(array.as_ref(), field.data_type(), rest, row)
}

fn variant_at_path_in_array(
    array: &dyn Array,
    data_type: &DataType,
    path: &[usize],
    row: usize,
) -> Result<Option<GenericVariant>> {
    if path.is_empty() {
        return variant_from_array(array, row);
    }

    let DataType::Row(row_type) = data_type else {
        return Err(Error::DataInvalid {
            message: "Variant inference path must traverse ROW fields".to_string(),
            source: None,
        });
    };
    if array.is_null(row) {
        return Ok(None);
    }
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant inference ROW value must be StructArray".to_string(),
            source: None,
        })?;
    let field_idx = path[0];
    let Some(field) = row_type.fields().get(field_idx) else {
        return Err(Error::DataInvalid {
            message: "Invalid nested Variant inference path".to_string(),
            source: None,
        });
    };
    let Some(child) = struct_array.columns().get(field_idx) else {
        return Err(Error::DataInvalid {
            message: "Nested Variant inference path is outside StructArray".to_string(),
            source: None,
        });
    };
    variant_at_path_in_array(child.as_ref(), field.data_type(), &path[1..], row)
}

fn variant_from_array(array: &dyn Array, row: usize) -> Result<Option<GenericVariant>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant inference value must be StructArray".to_string(),
            source: None,
        })?;
    let values = input
        .column_by_name(VARIANT_VALUE_FIELD_NAME)
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant.value column must be BinaryArray".to_string(),
            source: None,
        })?;
    let metadata = input
        .column_by_name(VARIANT_METADATA_FIELD_NAME)
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant.metadata column must be BinaryArray".to_string(),
            source: None,
        })?;
    GenericVariant::from_parts(values.value(row).to_vec(), metadata.value(row).to_vec()).map(Some)
}

fn update_inferred_variant_schema(
    row_type: &RowType,
    inferred_types: &HashMap<Vec<usize>, DataType>,
    path: &[usize],
) -> Result<RowType> {
    let mut fields = Vec::with_capacity(row_type.fields().len());
    for (idx, field) in row_type.fields().iter().enumerate() {
        let mut full_path = path.to_vec();
        full_path.push(idx);
        let data_type = match field.data_type() {
            DataType::Variant(_) => inferred_types
                .get(&full_path)
                .cloned()
                .unwrap_or_else(|| field.data_type().clone()),
            DataType::Row(child) => {
                let updated = update_inferred_variant_schema(child, inferred_types, &full_path)?;
                DataType::Row(updated).copy_with_nullable(field.data_type().is_nullable())?
            }
            _ => field.data_type().clone(),
        };
        fields.push(data_field_with_type(field, data_type));
    }
    Ok(RowType::new(fields))
}

fn physical_fields_for_configured_shredding(
    logical_fields: &[DataField],
    configured: &RowType,
) -> Result<Vec<DataField>> {
    logical_fields
        .iter()
        .map(|field| {
            let physical_type = match field.data_type() {
                DataType::Variant(_) => configured
                    .fields()
                    .iter()
                    .find(|configured_field| configured_field.name() == field.name())
                    .map(|configured_field| variant_shredding_type(configured_field.data_type()))
                    .transpose()?
                    .unwrap_or_else(|| field.data_type().clone()),
                _ => field.data_type().clone(),
            };
            Ok(data_field_with_type(field, physical_type))
        })
        .collect()
}

fn data_field_with_type(field: &DataField, data_type: DataType) -> DataField {
    DataField::new(field.id(), field.name().to_string(), data_type)
        .with_description(field.description().map(ToString::to_string))
}

pub(crate) fn batch_to_shredded_physical(
    batch: &RecordBatch,
    logical_fields: &[DataField],
    physical_fields: &[DataField],
) -> Result<RecordBatch> {
    if logical_fields == physical_fields {
        return Ok(batch.clone());
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, logical_field) in logical_fields.iter().enumerate() {
        let physical_field = &physical_fields[idx];
        columns.push(array_to_shredded_physical(
            batch.column(idx).as_ref(),
            logical_field.data_type(),
            physical_field.data_type(),
            logical_field.name(),
        )?);
    }

    let schema = build_target_arrow_schema(physical_fields)?;
    RecordBatch::try_new(schema, columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build shredded RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn array_to_shredded_physical(
    array: &dyn Array,
    logical_type: &DataType,
    physical_type: &DataType,
    column_name: &str,
) -> Result<ArrayRef> {
    if logical_type == physical_type {
        return Ok(array.slice(0, array.len()));
    }

    match (logical_type, physical_type) {
        (DataType::Variant(_), DataType::Row(_)) => shred_variant_array(array, physical_type),
        (DataType::Row(logical), DataType::Row(physical)) => {
            row_array_to_shredded_physical(array, logical, physical, column_name)
        }
        _ => Err(Error::Unsupported {
            message: format!("Unsupported variant shredding conversion for column '{column_name}'"),
        }),
    }
}

fn row_array_to_shredded_physical(
    array: &dyn Array,
    logical_type: &RowType,
    physical_type: &RowType,
    column_name: &str,
) -> Result<ArrayRef> {
    if logical_type.fields().len() != physical_type.fields().len() {
        return Err(Error::DataInvalid {
            message: format!("Logical and physical ROW field counts differ for '{column_name}'"),
            source: None,
        });
    }
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Variant shredding ROW column '{column_name}' must be StructArray"),
            source: None,
        })?;
    let fields: Fields = physical_type
        .fields()
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name(),
                paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into();
    let columns = logical_type
        .fields()
        .iter()
        .zip(physical_type.fields())
        .enumerate()
        .map(|(idx, (logical_field, physical_field))| {
            if logical_field.name() != physical_field.name() {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Logical and physical ROW field names differ for '{column_name}'"
                    ),
                    source: None,
                });
            }
            array_to_shredded_physical(
                input.column(idx).as_ref(),
                logical_field.data_type(),
                physical_field.data_type(),
                logical_field.name(),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    if fields.is_empty() {
        Ok(Arc::new(StructArray::new_empty_fields(
            input.len(),
            input.nulls().cloned(),
        )))
    } else {
        Ok(Arc::new(
            StructArray::try_new(fields, columns, input.nulls().cloned()).map_err(|e| {
                Error::UnexpectedError {
                    message: format!("Failed to build Variant shredded ROW column: {e}"),
                    source: Some(Box::new(e)),
                }
            })?,
        ))
    }
}

fn shred_variant_array(array: &dyn Array, physical_type: &DataType) -> Result<ArrayRef> {
    let DataType::Row(row_type) = physical_type else {
        return Err(Error::DataInvalid {
            message: format!("Variant shredding physical type must be ROW, got {physical_type:?}"),
            source: None,
        });
    };
    let schema = build_variant_schema(row_type)?;
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant column must be a StructArray".to_string(),
            source: None,
        })?;
    let values = input
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant.value column must be BinaryArray".to_string(),
            source: None,
        })?;
    let metadata = input
        .column_by_name("metadata")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant.metadata column must be BinaryArray".to_string(),
            source: None,
        })?;

    let mut rows = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            rows.push(None);
        } else {
            let variant = GenericVariant::from_parts(
                values.value(row).to_vec(),
                metadata.value(row).to_vec(),
            )?;
            rows.push(Some(cast_shredded(&variant, &schema)?));
        }
    }
    shredded_rows_to_struct_array(rows, row_type)
}

pub(crate) fn is_shredded_variant_array(array: &dyn Array) -> bool {
    let ArrowDataType::Struct(fields) = array.data_type() else {
        return false;
    };
    fields
        .iter()
        .any(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME)
}

pub(crate) fn assemble_shredded_variant_array(array: &dyn Array) -> Result<ArrayRef> {
    let row_type = match arrow_to_paimon_type(array.data_type(), true)? {
        DataType::Row(row) => row,
        other => {
            return Err(Error::DataInvalid {
                message: format!("Shredded Variant physical type must be ROW, got {other:?}"),
                source: None,
            })
        }
    };
    let schema = build_variant_schema(&row_type)?;
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Shredded Variant column must be a StructArray".to_string(),
            source: None,
        })?;

    let mut variants = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            variants.push(None);
        } else {
            let shredded =
                struct_row_at(input, row, &row_type)?.ok_or_else(|| Error::DataInvalid {
                    message: "Shredded Variant row is null".to_string(),
                    source: None,
                })?;
            variants.push(Some(rebuild_shredded(&shredded, &schema)?));
        }
    }
    variant_array(variants)
}

pub(crate) fn assemble_shredded_variant_batch(
    batch: RecordBatch,
    read_fields: &[DataField],
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut changed = false;
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut output_fields = Vec::with_capacity(batch.num_columns());
    let logical_schema = build_target_arrow_schema(read_fields)?;

    for (idx, arrow_field) in schema.fields().iter().enumerate() {
        let column = batch.column(idx);
        let logical_idx = read_fields
            .iter()
            .position(|field| field.name() == arrow_field.name());

        if let Some(field_idx) = logical_idx {
            if let Some(assembled) =
                assemble_array_to_logical(column.as_ref(), read_fields[field_idx].data_type())?
            {
                columns.push(assembled);
                output_fields.push(logical_schema.field(field_idx).clone());
                changed = true;
                continue;
            }
        }

        columns.push(column.clone());
        output_fields.push(arrow_field.as_ref().clone());
    }

    if !changed {
        return Ok(batch);
    }

    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(output_fields)), columns).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to build assembled Variant RecordBatch: {e}"),
            source: Some(Box::new(e)),
        }
    })
}

fn assemble_array_to_logical(
    array: &dyn Array,
    logical_type: &DataType,
) -> Result<Option<ArrayRef>> {
    match logical_type {
        DataType::Variant(_) if is_variant_storage_array(array) => {
            Ok(Some(assemble_shredded_variant_array(array)?))
        }
        DataType::Row(row_type)
            if is_variant_extraction_row(row_type)
                && (is_variant_storage_array(array) || is_plain_variant_array(array)) =>
        {
            Ok(Some(assemble_variant_extraction_array(array, row_type)?))
        }
        DataType::Row(row_type) => assemble_row_array_to_logical(array, row_type),
        _ => Ok(None),
    }
}

fn assemble_variant_extraction_array(array: &dyn Array, row_type: &RowType) -> Result<ArrayRef> {
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant extraction column must be StructArray".to_string(),
            source: None,
        })?;
    let fields = row_type.fields();
    let metadata = fields
        .iter()
        .map(|field| {
            let Some(description) = field.description() else {
                return Err(Error::DataInvalid {
                    message: "Variant extraction field is missing metadata".to_string(),
                    source: None,
                });
            };
            parse_variant_metadata(description)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut values_by_field = vec![Vec::with_capacity(input.len()); fields.len()];
    let mut validities = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            validities.push(false);
            for values in &mut values_by_field {
                values.push(None);
            }
            continue;
        }

        validities.push(true);
        let variant = variant_from_storage_row(input, row)?;
        for (field_idx, field) in fields.iter().enumerate() {
            let field_metadata = &metadata[field_idx];
            let value = match &variant {
                Some(variant) => match variant.get_path(field_metadata.path()) {
                    Ok(Some(extracted)) => cast_variant_to_shredded_value(
                        extracted,
                        field.data_type(),
                        field_metadata.fail_on_error(),
                    )?,
                    Ok(None) => None,
                    Err(e) if !field_metadata.fail_on_error() => {
                        let _ = e;
                        None
                    }
                    Err(e) => return Err(e),
                },
                None => None,
            };
            values_by_field[field_idx].push(value);
        }
    }

    let arrow_fields: Fields = fields
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name(),
                paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into();
    let columns = values_by_field
        .iter()
        .zip(fields)
        .map(|(values, field)| array_from_values(values, field.data_type()))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(
        StructArray::try_new(arrow_fields, columns, Some(null_buffer(validities))).map_err(
            |e| Error::UnexpectedError {
                message: format!("Failed to build Variant extraction StructArray: {e}"),
                source: Some(Box::new(e)),
            },
        )?,
    ))
}

fn variant_from_storage_row(input: &StructArray, row: usize) -> Result<Option<GenericVariant>> {
    if input.is_null(row) {
        return Ok(None);
    }
    if input
        .column_by_name(VARIANT_TYPED_VALUE_FIELD_NAME)
        .is_none()
    {
        return variant_from_array(input, row);
    }

    let row_type = match arrow_to_paimon_type(input.data_type(), true)? {
        DataType::Row(row_type) => row_type,
        DataType::Variant(_) => return variant_from_array(input, row),
        other => {
            return Err(Error::DataInvalid {
                message: format!("Variant storage physical type must be ROW, got {other:?}"),
                source: None,
            })
        }
    };
    let schema = build_variant_schema(&row_type)?;
    let shredded = struct_row_at(input, row, &row_type)?.ok_or_else(|| Error::DataInvalid {
        message: "Variant extraction storage row is null".to_string(),
        source: None,
    })?;
    rebuild_shredded(&shredded, &schema).map(Some)
}

fn assemble_row_array_to_logical(
    array: &dyn Array,
    row_type: &RowType,
) -> Result<Option<ArrayRef>> {
    let ArrowDataType::Struct(_) = array.data_type() else {
        return Ok(None);
    };
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Variant assembled ROW column must be StructArray".to_string(),
            source: None,
        })?;
    if input.num_columns() != row_type.fields().len() {
        return Ok(None);
    }

    let mut changed = false;
    let mut columns = Vec::with_capacity(input.num_columns());
    for (idx, field) in row_type.fields().iter().enumerate() {
        if let Some(assembled) =
            assemble_array_to_logical(input.column(idx).as_ref(), field.data_type())?
        {
            columns.push(assembled);
            changed = true;
        } else {
            columns.push(input.column(idx).clone());
        }
    }

    if !changed {
        return Ok(None);
    }

    let fields: Fields = row_type
        .fields()
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name(),
                paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into();
    if fields.is_empty() {
        Ok(Some(Arc::new(StructArray::new_empty_fields(
            input.len(),
            input.nulls().cloned(),
        ))))
    } else {
        Ok(Some(Arc::new(
            StructArray::try_new(fields, columns, input.nulls().cloned()).map_err(|e| {
                Error::UnexpectedError {
                    message: format!("Failed to build assembled Variant ROW column: {e}"),
                    source: Some(Box::new(e)),
                }
            })?,
        )))
    }
}

fn is_variant_storage_array(array: &dyn Array) -> bool {
    let ArrowDataType::Struct(fields) = array.data_type() else {
        return false;
    };
    let has_binary = |name: &str| {
        fields
            .iter()
            .any(|field| field.name() == name && field.data_type() == &ArrowDataType::Binary)
    };
    has_binary(VARIANT_VALUE_FIELD_NAME)
        && has_binary(VARIANT_METADATA_FIELD_NAME)
        && (!is_variant_arrow_fields(fields) || is_shredded_variant_array(array))
}

fn is_plain_variant_array(array: &dyn Array) -> bool {
    let ArrowDataType::Struct(fields) = array.data_type() else {
        return false;
    };
    is_variant_arrow_fields(fields)
}

fn shredded_rows_to_struct_array(
    rows: Vec<Option<ShreddedRow>>,
    row_type: &RowType,
) -> Result<ArrayRef> {
    let values = rows
        .into_iter()
        .map(|row| row.map(ShreddedValue::Row))
        .collect::<Vec<_>>();
    array_from_values(&values, &DataType::Row(row_type.clone()))
}

fn array_from_values(values: &[Option<ShreddedValue>], data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean(_) => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Boolean(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::TinyInt(_) => Ok(Arc::new(Int8Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Int8(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::SmallInt(_) => Ok(Arc::new(Int16Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Int16(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Int(_) => Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Int32(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::BigInt(_) => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Int64(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Float(_) => Ok(Arc::new(Float32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Float32(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Double(_) => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Float64(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Decimal(decimal) => {
            let array = Decimal128Array::from(
                values
                    .iter()
                    .map(|value| match value {
                        Some(ShreddedValue::Decimal128(v)) => Some(*v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(decimal.precision() as u8, decimal.scale() as i8)
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to build Variant decimal typed_value: {e}"),
                source: Some(Box::new(e)),
            })?;
            Ok(Arc::new(array))
        }
        DataType::VarChar(_) | DataType::Char(_) => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::String(v)) => Some(v.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::VarBinary(_) | DataType::Binary(_) | DataType::Blob(_) => {
            let values = values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Binary(v)) => Some(v.as_slice()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        DataType::Date(_) => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Date32(v)) => Some(*v),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        DataType::Timestamp(ts) => timestamp_array(values, ts.precision(), None),
        DataType::LocalZonedTimestamp(ts) => timestamp_array(values, ts.precision(), Some("UTC")),
        DataType::Row(row_type) => struct_array_from_values(values, row_type),
        DataType::Array(array_type) => list_array_from_values(values, array_type),
        other => Ok(new_null_array(&paimon_type_to_arrow(other)?, values.len())),
    }
}

fn timestamp_array(
    values: &[Option<ShreddedValue>],
    precision: u32,
    tz: Option<&str>,
) -> Result<ArrayRef> {
    let values = values
        .iter()
        .map(|value| match value {
            Some(ShreddedValue::Timestamp(v)) => Some(*v),
            _ => None,
        })
        .collect::<Vec<_>>();
    Ok(match precision {
        0..=3 => Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz)),
        4..=6 => Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz)),
        _ => Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz)),
    })
}

fn struct_array_from_values(
    values: &[Option<ShreddedValue>],
    row_type: &RowType,
) -> Result<ArrayRef> {
    let fields: Fields = row_type
        .fields()
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name(),
                paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into();
    let validities = values.iter().map(Option::is_some).collect::<Vec<_>>();
    let columns = row_type
        .fields()
        .iter()
        .enumerate()
        .map(|(field_idx, field)| {
            let child_values = values
                .iter()
                .map(|value| match value {
                    Some(ShreddedValue::Row(row)) => {
                        row.fields().get(field_idx).cloned().unwrap_or(None)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            array_from_values(&child_values, field.data_type())
        })
        .collect::<Result<Vec<_>>>()?;
    let nulls = Some(null_buffer(validities));
    if fields.is_empty() {
        Ok(Arc::new(StructArray::new_empty_fields(values.len(), nulls)))
    } else {
        Ok(Arc::new(
            StructArray::try_new(fields, columns, nulls).map_err(|e| Error::UnexpectedError {
                message: format!("Failed to build Variant shredded StructArray: {e}"),
                source: Some(Box::new(e)),
            })?,
        ))
    }
}

fn list_array_from_values(
    values: &[Option<ShreddedValue>],
    array_type: &ArrayType,
) -> Result<ArrayRef> {
    let element_type = array_type.element_type();
    let element_arrow_type = paimon_type_to_arrow(element_type)?;
    let element_field = Arc::new(ArrowField::new(
        "element",
        element_arrow_type,
        element_type.is_nullable(),
    ));
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0i32);
    let mut validities = Vec::with_capacity(values.len());
    let mut flattened = Vec::new();
    for value in values {
        match value {
            Some(ShreddedValue::List(rows)) => {
                validities.push(true);
                for row in rows {
                    flattened.push(Some(ShreddedValue::Row(row.clone())));
                }
                let next = *offsets.last().unwrap()
                    + i32::try_from(rows.len()).map_err(|e| Error::DataInvalid {
                        message: "Variant shredded array has too many elements".to_string(),
                        source: Some(Box::new(e)),
                    })?;
                offsets.push(next);
            }
            _ => {
                validities.push(false);
                offsets.push(*offsets.last().unwrap());
            }
        }
    }
    let child = array_from_values(&flattened, element_type)?;
    Ok(Arc::new(
        ListArray::try_new(
            element_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            child,
            Some(null_buffer(validities)),
        )
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build Variant shredded ListArray: {e}"),
            source: Some(Box::new(e)),
        })?,
    ))
}

fn struct_row_at(
    array: &StructArray,
    row: usize,
    row_type: &RowType,
) -> Result<Option<ShreddedRow>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut shredded = ShreddedRow::new(row_type.fields().len());
    for (field_idx, field) in row_type.fields().iter().enumerate() {
        if let Some(value) = value_at(array.column(field_idx).as_ref(), row, field.data_type())? {
            shredded.set(field_idx, value)?;
        }
    }
    Ok(Some(shredded))
}

fn value_at(array: &dyn Array, row: usize, data_type: &DataType) -> Result<Option<ShreddedValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(match data_type {
        DataType::Boolean(_) => Some(ShreddedValue::Boolean(
            downcast_array::<BooleanArray>(array, "Boolean")?.value(row),
        )),
        DataType::TinyInt(_) => Some(ShreddedValue::Int8(
            downcast_array::<Int8Array>(array, "TinyInt")?.value(row),
        )),
        DataType::SmallInt(_) => Some(ShreddedValue::Int16(
            downcast_array::<Int16Array>(array, "SmallInt")?.value(row),
        )),
        DataType::Int(_) => Some(ShreddedValue::Int32(
            downcast_array::<Int32Array>(array, "Int")?.value(row),
        )),
        DataType::BigInt(_) => Some(ShreddedValue::Int64(
            downcast_array::<Int64Array>(array, "BigInt")?.value(row),
        )),
        DataType::Float(_) => Some(ShreddedValue::Float32(
            downcast_array::<Float32Array>(array, "Float")?.value(row),
        )),
        DataType::Double(_) => Some(ShreddedValue::Float64(
            downcast_array::<Float64Array>(array, "Double")?.value(row),
        )),
        DataType::Decimal(_) => Some(ShreddedValue::Decimal128(
            downcast_array::<Decimal128Array>(array, "Decimal")?.value(row),
        )),
        DataType::VarChar(_) | DataType::Char(_) => Some(ShreddedValue::String(
            downcast_array::<StringArray>(array, "String")?
                .value(row)
                .to_string(),
        )),
        DataType::VarBinary(_) | DataType::Binary(_) | DataType::Blob(_) => {
            Some(ShreddedValue::Binary(
                downcast_array::<BinaryArray>(array, "Binary")?
                    .value(row)
                    .to_vec(),
            ))
        }
        DataType::Date(_) => Some(ShreddedValue::Date32(
            downcast_array::<Date32Array>(array, "Date")?.value(row),
        )),
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            Some(ShreddedValue::Timestamp(timestamp_value_at(array, row)?))
        }
        DataType::Row(row_type) => {
            let struct_array = downcast_array::<StructArray>(array, "Struct")?;
            struct_row_at(struct_array, row, row_type)?.map(ShreddedValue::Row)
        }
        DataType::Array(array_type) => {
            let list = downcast_array::<ListArray>(array, "List")?;
            let child = list.value(row);
            let DataType::Row(element_row_type) = array_type.element_type() else {
                return Err(Error::DataInvalid {
                    message: "Variant shredded array element must be ROW".to_string(),
                    source: None,
                });
            };
            let child_struct = child
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: "Variant shredded array values must be StructArray".to_string(),
                    source: None,
                })?;
            let mut rows = Vec::with_capacity(child_struct.len());
            for idx in 0..child_struct.len() {
                rows.push(
                    struct_row_at(child_struct, idx, element_row_type)?.ok_or_else(|| {
                        Error::DataInvalid {
                            message: "Variant shredded array element row is null".to_string(),
                            source: None,
                        }
                    })?,
                );
            }
            Some(ShreddedValue::List(rows))
        }
        _ => None,
    })
}

fn timestamp_value_at(array: &dyn Array, row: usize) -> Result<i64> {
    match array.data_type() {
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(downcast_array::<TimestampMillisecondArray>(array, "TimestampMs")?.value(row))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(downcast_array::<TimestampMicrosecondArray>(array, "TimestampUs")?.value(row))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(downcast_array::<TimestampNanosecondArray>(array, "TimestampNs")?.value(row))
        }
        other => Err(Error::DataInvalid {
            message: format!("Unsupported Variant shredded timestamp array: {other:?}"),
            source: None,
        }),
    }
}

fn downcast_array<'a, T: 'static>(array: &'a dyn Array, name: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Expected Variant shredded {name} array, got {:?}",
                array.data_type()
            ),
            source: None,
        })
}

fn variant_array(values: Vec<Option<GenericVariant>>) -> Result<ArrayRef> {
    let len = values.len();
    let mut value_items: Vec<Option<&[u8]>> = Vec::with_capacity(len);
    let mut metadata_items: Vec<Option<&[u8]>> = Vec::with_capacity(len);
    let mut validities = Vec::with_capacity(len);
    for value in &values {
        match value {
            Some(variant) => {
                value_items.push(Some(variant.value()));
                metadata_items.push(Some(variant.metadata()));
                validities.push(true);
            }
            None => {
                value_items.push(Some(&[]));
                metadata_items.push(Some(&[]));
                validities.push(false);
            }
        }
    }
    Ok(Arc::new(
        StructArray::try_new(
            match crate::arrow::variant_arrow_type() {
                ArrowDataType::Struct(fields) => fields,
                _ => unreachable!("variant_arrow_type always returns Struct"),
            },
            vec![
                Arc::new(BinaryArray::from(value_items)),
                Arc::new(BinaryArray::from(metadata_items)),
            ],
            Some(null_buffer(validities)),
        )
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build Variant array: {e}"),
            source: Some(Box::new(e)),
        })?,
    ))
}

fn null_buffer(validities: Vec<bool>) -> NullBuffer {
    NullBuffer::new(BooleanBuffer::from(validities))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::variant_arrow_type;
    use crate::spec::{variant_extraction_row, IntType, VarCharType, VariantType};

    fn variant_array_for_test(values: &[GenericVariant]) -> ArrayRef {
        let value_items = values
            .iter()
            .map(|variant| Some(variant.value()))
            .collect::<Vec<_>>();
        let metadata_items = values
            .iter()
            .map(|variant| Some(variant.metadata()))
            .collect::<Vec<_>>();
        let fields = match variant_arrow_type() {
            ArrowDataType::Struct(fields) => fields,
            _ => unreachable!("variant_arrow_type is a struct"),
        };
        Arc::new(
            StructArray::try_new(
                fields,
                vec![
                    Arc::new(BinaryArray::from(value_items)),
                    Arc::new(BinaryArray::from(metadata_items)),
                ],
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn configured_schema_transforms_and_reassembles_variant_batch() {
        let logical_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "v".to_string(), DataType::Variant(VariantType::new())),
        ];
        let options = HashMap::from([(
            "parquet.variant.shreddingSchema".to_string(),
            r#"{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[{"name":"age","type":"BIGINT"},{"name":"city","type":"STRING"}]}}]}"#.to_string(),
        )]);
        let physical_fields = configured_variant_shredding_fields(&logical_fields, &options)
            .unwrap()
            .expect("shredding fields");
        assert_ne!(logical_fields, physical_fields);

        let variants = vec![
            GenericVariant::parse_json(r#"{"age":27,"city":"Beijing"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"city":"Hangzhou","other":"x"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":"old"}"#).unwrap(),
        ];
        let batch = RecordBatch::try_new(
            build_target_arrow_schema(&logical_fields).unwrap(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                variant_array_for_test(&variants),
            ],
        )
        .unwrap();

        let physical =
            batch_to_shredded_physical(&batch, &logical_fields, &physical_fields).unwrap();
        assert!(is_shredded_variant_array(physical.column(1).as_ref()));

        let assembled = assemble_shredded_variant_array(physical.column(1).as_ref()).unwrap();
        let assembled = assembled.as_any().downcast_ref::<StructArray>().unwrap();
        let values = assembled
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata = assembled
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for (idx, expected) in variants.iter().enumerate() {
            let actual = GenericVariant::from_parts(
                values.value(idx).to_vec(),
                metadata.value(idx).to_vec(),
            )
            .unwrap();
            assert_eq!(actual.to_json().unwrap(), expected.to_json().unwrap());
        }
    }

    #[test]
    fn configured_schema_noops_when_no_variant_field_matches() {
        let logical_fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];
        let options = HashMap::from([(
            "variant.shreddingSchema".to_string(),
            r#"{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[{"name":"age","type":"BIGINT"}]}}]}"#.to_string(),
        )]);
        assert!(
            configured_variant_shredding_fields(&logical_fields, &options)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn variant_extraction_row_reads_plain_variant_array() {
        let variants = vec![
            GenericVariant::parse_json(r#"{"age":27,"name":"Alice"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":"old"}"#).unwrap(),
        ];
        let array = variant_array_for_test(&variants);
        let row_type = variant_extraction_row(
            true,
            vec![
                (
                    DataType::Int(IntType::new()),
                    "$.age".to_string(),
                    false,
                    "UTC".to_string(),
                ),
                (
                    DataType::VarChar(VarCharType::string_type()),
                    "$.name".to_string(),
                    true,
                    "UTC".to_string(),
                ),
            ],
        );

        let extracted = assemble_array_to_logical(array.as_ref(), &DataType::Row(row_type))
            .unwrap()
            .expect("extracted");
        let extracted = extracted.as_any().downcast_ref::<StructArray>().unwrap();
        let ages = extracted
            .column_by_name("0")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = extracted
            .column_by_name("1")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(ages.value(0), 27);
        assert!(ages.is_null(1));
        assert_eq!(names.value(0), "Alice");
        assert!(names.is_null(1));
    }

    #[test]
    fn variant_extraction_row_reads_shredded_variant_array() {
        let logical_fields = vec![DataField::new(
            1,
            "v".to_string(),
            DataType::Variant(VariantType::new()),
        )];
        let options = HashMap::from([(
            "variant.shreddingSchema".to_string(),
            r#"{"type":"ROW","fields":[{"name":"v","type":{"type":"ROW","fields":[{"name":"age","type":"INT"},{"name":"name","type":"STRING"}]}}]}"#.to_string(),
        )]);
        let physical_fields = configured_variant_shredding_fields(&logical_fields, &options)
            .unwrap()
            .expect("shredding fields");
        let variants = vec![
            GenericVariant::parse_json(r#"{"age":27,"name":"Alice"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":"old","name":"Bob"}"#).unwrap(),
        ];
        let batch = RecordBatch::try_new(
            build_target_arrow_schema(&logical_fields).unwrap(),
            vec![variant_array_for_test(&variants)],
        )
        .unwrap();
        let physical =
            batch_to_shredded_physical(&batch, &logical_fields, &physical_fields).unwrap();
        let extraction_row_type = variant_extraction_row(
            true,
            vec![
                (
                    DataType::Int(IntType::new()),
                    "$.age".to_string(),
                    false,
                    "UTC".to_string(),
                ),
                (
                    DataType::VarChar(VarCharType::string_type()),
                    "$.name".to_string(),
                    true,
                    "UTC".to_string(),
                ),
            ],
        );
        let read_fields = vec![DataField::new(
            1,
            "v".to_string(),
            DataType::Row(extraction_row_type),
        )];

        let extracted_batch = assemble_shredded_variant_batch(physical, &read_fields).unwrap();
        let extracted = extracted_batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let ages = extracted
            .column_by_name("0")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = extracted
            .column_by_name("1")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(ages.value(0), 27);
        assert!(ages.is_null(1));
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
    }

    #[test]
    fn inferred_schema_transforms_and_reassembles_variant_batch() {
        let logical_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "v".to_string(), DataType::Variant(VariantType::new())),
        ];
        let variants = vec![
            GenericVariant::parse_json(r#"{"age":30,"name":"Alice"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":25,"name":"Bob"}"#).unwrap(),
            GenericVariant::parse_json(r#"{"age":35,"name":"Charlie"}"#).unwrap(),
        ];
        let batch = RecordBatch::try_new(
            build_target_arrow_schema(&logical_fields).unwrap(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                variant_array_for_test(&variants),
            ],
        )
        .unwrap();
        let options = HashMap::from([(
            "variant.inferShreddingSchema".to_string(),
            "true".to_string(),
        )]);

        let physical_fields =
            infer_variant_shredding_fields(&logical_fields, std::slice::from_ref(&batch), &options)
                .unwrap()
                .expect("inferred shredding fields");
        let DataType::Row(variant_row) = physical_fields[1].data_type() else {
            panic!("expected inferred variant field to become ROW");
        };
        let typed_value = variant_row
            .fields()
            .iter()
            .find(|field| field.name() == VARIANT_TYPED_VALUE_FIELD_NAME)
            .expect("typed_value field");
        let DataType::Row(object_row) = typed_value.data_type() else {
            panic!("expected typed_value object ROW");
        };
        assert_eq!(
            object_row
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            vec!["age", "name"]
        );

        let physical =
            batch_to_shredded_physical(&batch, &logical_fields, &physical_fields).unwrap();
        assert!(is_shredded_variant_array(physical.column(1).as_ref()));
        let assembled =
            assemble_shredded_variant_batch(physical, &logical_fields).expect("assembled");
        let assembled = assembled
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let values = assembled
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let metadata = assembled
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for (idx, expected) in variants.iter().enumerate() {
            let actual = GenericVariant::from_parts(
                values.value(idx).to_vec(),
                metadata.value(idx).to_vec(),
            )
            .unwrap();
            assert_eq!(actual.to_json().unwrap(), expected.to_json().unwrap());
        }
    }

    #[test]
    fn inferred_schema_converts_nested_row_variant() {
        let nested_row = RowType::new(vec![DataField::new(
            0,
            "v".to_string(),
            DataType::Variant(VariantType::new()),
        )]);
        let logical_fields = vec![DataField::new(
            0,
            "payload".to_string(),
            DataType::Row(nested_row),
        )];
        let variants = vec![
            GenericVariant::parse_json(r#"{"score":1}"#).unwrap(),
            GenericVariant::parse_json(r#"{"score":2}"#).unwrap(),
        ];
        let row_arrow_type = paimon_type_to_arrow(logical_fields[0].data_type()).unwrap();
        let ArrowDataType::Struct(row_fields) = row_arrow_type else {
            panic!("expected ROW arrow type");
        };
        let payload =
            StructArray::try_new(row_fields, vec![variant_array_for_test(&variants)], None)
                .unwrap();
        let batch = RecordBatch::try_new(
            build_target_arrow_schema(&logical_fields).unwrap(),
            vec![Arc::new(payload)],
        )
        .unwrap();
        let options = HashMap::from([(
            "variant.inferShreddingSchema".to_string(),
            "true".to_string(),
        )]);

        let physical_fields =
            infer_variant_shredding_fields(&logical_fields, std::slice::from_ref(&batch), &options)
                .unwrap()
                .expect("nested inferred shredding fields");
        let physical =
            batch_to_shredded_physical(&batch, &logical_fields, &physical_fields).unwrap();
        let assembled =
            assemble_shredded_variant_batch(physical, &logical_fields).expect("assembled");
        assert_eq!(
            assembled.schema().field(0).data_type(),
            build_target_arrow_schema(&logical_fields)
                .unwrap()
                .field(0)
                .data_type()
        );
    }
}
