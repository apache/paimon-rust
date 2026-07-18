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

//! MAP shared-shredding (PIP-43), compatible with Java's
//! `org.apache.paimon.data.shredding.MapSharedShredding*`.
//!
//! A logical `MAP<STRING, T>` field is stored physically as
//! `ROW<__field_mapping: ARRAY<INT>, __col_0..__col_{K-1}: T, __overflow: MAP<INT, T>>`:
//! the first `K` entries of each row (in row order) go to the shared columns,
//! the rest go to the overflow map keyed by a file-local field id. The field
//! dictionary and column statistics are committed into the `ARROW:schema`
//! footer metadata at close time so readers can rebuild the logical maps.

use super::{option_usize, FieldMetadata, ShreddingReadPlan, ShreddingWritePlan};
use crate::arrow::{build_target_arrow_schema, paimon_type_to_arrow};
use crate::spec::{ArrayType, DataField, DataType, IntType, MapType, RowType};
use crate::{Error, Result};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    UInt32Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Fields, Schema as ArrowSchema};
use arrow_select::interleave::interleave;
use arrow_select::take::take;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Metadata keys (mirroring Java's MapShreddingDefine / MapSharedShreddingDefine)
// ---------------------------------------------------------------------------

pub(crate) const MAP_STORAGE_LAYOUT_KEY: &str = "paimon.map.storage-layout";
pub(crate) const MAP_STORAGE_LAYOUT_SHARED_SHREDDING: &str = "shared-shredding";
const VERSION_KEY: &str = "paimon.map.shared-shredding.version";
const CURRENT_VERSION: i32 = 1;
const FIELD_DICT_KEY: &str = "paimon.map.shared-shredding.field-dict";
const FIELD_DICT_COMPRESSION_KEY: &str = "paimon.map.shared-shredding.field-dict-compression";
const FIELD_DICT_ORIGINAL_SIZE_KEY: &str = "paimon.map.shared-shredding.field-dict-original-size";
const FIELD_COLUMNS_KEY: &str = "paimon.map.shared-shredding.field-columns";
const OVERFLOW_SET_KEY: &str = "paimon.map.shared-shredding.overflow-set";
const NUM_COLUMNS_KEY: &str = "paimon.map.shared-shredding.num-columns";
const MAX_ROW_WIDTH_KEY: &str = "paimon.map.shared-shredding.max-row-width";

const FIELD_MAPPING_NAME: &str = "__field_mapping";
const OVERFLOW_NAME: &str = "__overflow";

fn physical_column_name(index: usize) -> String {
    format!("__col_{index}")
}

// ---------------------------------------------------------------------------
// Options (mirroring Java's CoreOptions MAP_STORAGE_LAYOUT /
// MAP_SHARED_SHREDDING_MAX_COLUMNS accessed as `fields.<name>.<key>`)
// ---------------------------------------------------------------------------

const MAP_STORAGE_LAYOUT_OPTION_SUFFIX: &str = "map.storage-layout";
const MAP_SHARED_SHREDDING_MAX_COLUMNS_OPTION_SUFFIX: &str = "map.shared-shredding.max-columns";
const DEFAULT_MAP_SHARED_SHREDDING_MAX_COLUMNS: usize = 256;

/// Mirrors Java's `MapSharedShreddingWritePlanFactory.INFER_BUFFER_ROW_COUNT`.
pub(crate) const MAP_SHREDDING_INFER_BUFFER_ROW_COUNT: usize = 1;

/// One top-level field configured for shared-shredding.
pub(crate) struct MapShreddingFieldConfig {
    /// Index of the field in the logical write fields.
    pub(crate) field_index: usize,
    pub(crate) field_name: String,
    pub(crate) max_columns: usize,
}

/// Whether the type is a MAP with a VARCHAR key, mirroring Java's
/// `MapSharedShreddingUtils.isShreddingKeyMap`.
pub(crate) fn is_shredding_key_map(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(map_type) if matches!(map_type.key_type(), DataType::VarChar(_)))
}

/// Detect top-level fields configured with
/// `fields.<name>.map.storage-layout=shared-shredding`, mirroring Java's
/// `MapSharedShreddingUtils.detectShreddingColumns` + `buildColumnToNumColumns`.
pub(crate) fn detect_map_shredding_fields(
    fields: &[DataField],
    options: &HashMap<String, String>,
) -> Result<Vec<MapShreddingFieldConfig>> {
    let mut configs = Vec::new();
    for (field_index, field) in fields.iter().enumerate() {
        if !is_shredding_key_map(field.data_type()) {
            continue;
        }
        let layout_key = format!(
            "fields.{}.{}",
            field.name(),
            MAP_STORAGE_LAYOUT_OPTION_SUFFIX
        );
        let Some(layout) = options.get(&layout_key) else {
            continue;
        };
        if layout.eq_ignore_ascii_case("default") {
            continue;
        }
        if !layout.eq_ignore_ascii_case(MAP_STORAGE_LAYOUT_SHARED_SHREDDING) {
            return Err(Error::DataInvalid {
                message: format!(
                    "Invalid value '{layout}' for option '{layout_key}': expected 'default' or 'shared-shredding'"
                ),
                source: None,
            });
        }
        let max_columns_key = format!(
            "fields.{}.{}",
            field.name(),
            MAP_SHARED_SHREDDING_MAX_COLUMNS_OPTION_SUFFIX
        );
        let max_columns = option_usize(
            options,
            &max_columns_key,
            DEFAULT_MAP_SHARED_SHREDDING_MAX_COLUMNS,
        )?;
        if max_columns == 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    "options {MAP_SHARED_SHREDDING_MAX_COLUMNS_OPTION_SUFFIX} must > 0"
                ),
                source: None,
            });
        }
        configs.push(MapShreddingFieldConfig {
            field_index,
            field_name: field.name().to_string(),
            max_columns,
        });
    }
    Ok(configs)
}

// ---------------------------------------------------------------------------
// Field dictionary (mirroring Java's MapSharedShreddingFieldDict)
// ---------------------------------------------------------------------------

/// File-local field name -> field id dictionary for one shared-shredding MAP column.
struct FieldDict {
    name_to_id: BTreeMap<String, i32>,
    next_id: i32,
}

impl FieldDict {
    fn new() -> Self {
        Self {
            name_to_id: BTreeMap::new(),
            next_id: 0,
        }
    }

    fn get_or_assign(&mut self, name: &str) -> i32 {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }
        let new_id = self.next_id;
        self.next_id += 1;
        self.name_to_id.insert(name.to_string(), new_id);
        new_id
    }
}

// ---------------------------------------------------------------------------
// Column allocator (mirroring Java's MapSharedShreddingColumnAllocator)
// ---------------------------------------------------------------------------

/// Per-row physical column allocation for one row.
struct RowAllocation {
    /// `col_to_field[i]` = field id stored in physical column `i`, -1 for empty.
    col_to_field: Vec<i32>,
    /// Field ids stored in the overflow map, in row order.
    overflow_fields: Vec<i32>,
}

/// Per-row physical column allocator for one shared-shredding MAP column.
///
/// Assigns fields to physical columns by row order, mirroring the (temporary)
/// simple Java implementation.
struct ColumnAllocator {
    num_columns: usize,
    field_to_columns: BTreeMap<i32, BTreeSet<usize>>,
    overflow_field_set: BTreeSet<i32>,
    max_row_width: usize,
}

impl ColumnAllocator {
    fn new(num_columns: usize) -> Self {
        Self {
            num_columns,
            field_to_columns: BTreeMap::new(),
            overflow_field_set: BTreeSet::new(),
            max_row_width: 0,
        }
    }

    fn allocate_row(&mut self, field_ids: &[i32]) -> RowAllocation {
        self.max_row_width = self.max_row_width.max(field_ids.len());

        let mut col_to_field = vec![-1; self.num_columns];
        let assign_limit = field_ids.len().min(self.num_columns);
        for (i, &field_id) in field_ids.iter().take(assign_limit).enumerate() {
            col_to_field[i] = field_id;
            self.field_to_columns.entry(field_id).or_default().insert(i);
        }

        let mut overflow_fields = Vec::new();
        for &field_id in field_ids.iter().skip(assign_limit) {
            overflow_fields.push(field_id);
            self.overflow_field_set.insert(field_id);
        }

        RowAllocation {
            col_to_field,
            overflow_fields,
        }
    }
}

// ---------------------------------------------------------------------------
// Field metadata (mirroring Java's MapSharedShreddingFieldMeta)
// ---------------------------------------------------------------------------

/// File-level shredding metadata for one MAP column.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MapSharedShreddingFieldMeta {
    /// Field name -> field id, sorted by name.
    name_to_id: BTreeMap<String, i32>,
    /// Field id -> sorted physical column indices.
    field_to_columns: BTreeMap<i32, Vec<usize>>,
    /// Field ids that ever went to the overflow map, sorted.
    overflow_set: BTreeSet<i32>,
    num_columns: usize,
    max_row_width: usize,
}

// ---------------------------------------------------------------------------
// Physical schema (mirroring Java's MapSharedShreddingUtils.build*StructType)
// ---------------------------------------------------------------------------

/// Build the physical `ROW<__field_mapping, __col_0.., [__overflow]>` type for
/// a MAP value type. The writer always includes the overflow column; readers
/// include it only when the overflow set is non-empty (mirroring Java's
/// `buildPhysicalStructType` / `buildSpecificPhysicalStructType`).
fn build_physical_struct_type(
    value_type: &DataType,
    num_columns: usize,
    include_overflow: bool,
) -> Result<DataType> {
    let mut fields = Vec::with_capacity(num_columns + 2);
    fields.push(DataField::new(
        0,
        FIELD_MAPPING_NAME.to_string(),
        DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
    ));
    let direct_value_type = value_type.copy_with_nullable(true)?;
    for i in 0..num_columns {
        fields.push(DataField::new(
            (i + 1) as i32,
            physical_column_name(i),
            direct_value_type.clone(),
        ));
    }
    if include_overflow {
        fields.push(DataField::new(
            (num_columns + 1) as i32,
            OVERFLOW_NAME.to_string(),
            DataType::Map(MapType::new(
                DataType::Int(IntType::new()),
                value_type.clone(),
            )),
        ));
    }
    Ok(DataType::Row(RowType::new(fields)))
}

// ---------------------------------------------------------------------------
// Metadata serde (mirroring Java's MapSharedShreddingUtils.serialize/deserialize)
// ---------------------------------------------------------------------------

pub(crate) fn has_shredding_metadata(metadata: Option<&HashMap<String, String>>) -> bool {
    metadata
        .and_then(|md| md.get(MAP_STORAGE_LAYOUT_KEY))
        .is_some_and(|layout| layout == MAP_STORAGE_LAYOUT_SHARED_SHREDDING)
}

/// Normalize the field-dict compression, mirroring Java's
/// `normalizeFieldDictCompression`: `None` defaults to zstd and only
/// none/lz4/zstd are accepted.
pub(crate) fn normalize_field_dict_compression(compression: Option<&str>) -> Result<&'static str> {
    let Some(compression) = compression else {
        return Ok("zstd");
    };
    match compression.to_ascii_lowercase().as_str() {
        "none" => Ok("none"),
        "lz4" => Ok("lz4"),
        "zstd" => Ok("zstd"),
        other => Err(Error::DataInvalid {
            message: format!(
                "MAP shared-shredding only supports none/lz4/zstd compression, but is {other}."
            ),
            source: None,
        }),
    }
}

/// Serialize one field's shredding metadata, mirroring Java's
/// `MapSharedShreddingUtils.serializeMetadata`.
fn serialize_metadata(
    field_meta: &MapSharedShreddingFieldMeta,
    compression: Option<&str>,
) -> Result<HashMap<String, String>> {
    let compression = normalize_field_dict_compression(compression)?;
    let field_dict_json =
        serde_json::to_string(&field_meta.name_to_id).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to serialize shared-shredding metadata: {e}"),
            source: Some(Box::new(e)),
        })?;
    let field_dict_bytes = field_dict_json.as_bytes();

    let mut metadata = HashMap::new();
    metadata.insert(
        MAP_STORAGE_LAYOUT_KEY.to_string(),
        MAP_STORAGE_LAYOUT_SHARED_SHREDDING.to_string(),
    );
    metadata.insert(VERSION_KEY.to_string(), CURRENT_VERSION.to_string());
    metadata.insert(
        FIELD_DICT_COMPRESSION_KEY.to_string(),
        compression.to_string(),
    );
    metadata.insert(
        FIELD_DICT_ORIGINAL_SIZE_KEY.to_string(),
        field_dict_bytes.len().to_string(),
    );
    metadata.insert(
        FIELD_DICT_KEY.to_string(),
        bytes_to_string(&compress_dict(field_dict_bytes, compression)?),
    );
    metadata.insert(
        FIELD_COLUMNS_KEY.to_string(),
        serde_json::to_string(&field_meta.field_to_columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to serialize shared-shredding metadata: {e}"),
                source: Some(Box::new(e)),
            }
        })?,
    );
    metadata.insert(
        OVERFLOW_SET_KEY.to_string(),
        serde_json::to_string(&field_meta.overflow_set).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to serialize shared-shredding metadata: {e}"),
            source: Some(Box::new(e)),
        })?,
    );
    metadata.insert(
        NUM_COLUMNS_KEY.to_string(),
        field_meta.num_columns.to_string(),
    );
    metadata.insert(
        MAX_ROW_WIDTH_KEY.to_string(),
        field_meta.max_row_width.to_string(),
    );
    Ok(metadata)
}

const MAX_FIELD_DICT_SIZE: usize = 16 * 1024 * 1024;
const MAX_ENCODED_FIELD_DICT_SIZE: usize = 2 * MAX_FIELD_DICT_SIZE;
const MAX_SHARED_SHREDDING_NUM_COLUMNS: usize = 16 * 1024;
const MAX_SHARED_SHREDDING_ROW_WIDTH: usize = 1_000_000;

/// Deserialize one field's shredding metadata, mirroring Java's
/// `MapSharedShreddingUtils.deserializeMetadata`.
pub(crate) fn deserialize_metadata(
    metadata: &HashMap<String, String>,
) -> Result<MapSharedShreddingFieldMeta> {
    if !has_shredding_metadata(Some(metadata)) {
        return Err(Error::DataInvalid {
            message: "metadata is null or storage layout is not shared-shredding".to_string(),
            source: None,
        });
    }

    let version = required_int(metadata, VERSION_KEY)?;
    if version != CURRENT_VERSION {
        return Err(Error::DataInvalid {
            message: format!(
                "unsupported shared-shredding metadata version: {version}, expected: {CURRENT_VERSION}"
            ),
            source: None,
        });
    }

    let declared_original_len = required_int(metadata, FIELD_DICT_ORIGINAL_SIZE_KEY)?;
    let original_len = usize::try_from(declared_original_len).map_err(|_| Error::DataInvalid {
        message: format!(
            "malformed shredding metadata: field dictionary original size must be non-negative, got {declared_original_len}"
        ),
        source: None,
    })?;
    if original_len > MAX_FIELD_DICT_SIZE {
        return Err(Error::DataInvalid {
            message: format!(
                "malformed shredding metadata: field dictionary original size {original_len} exceeds maximum {MAX_FIELD_DICT_SIZE}"
            ),
            source: None,
        });
    }

    let num_columns =
        required_bounded_usize(metadata, NUM_COLUMNS_KEY, MAX_SHARED_SHREDDING_NUM_COLUMNS)?;
    let max_row_width =
        required_bounded_usize(metadata, MAX_ROW_WIDTH_KEY, MAX_SHARED_SHREDDING_ROW_WIDTH)?;

    let compression = normalize_field_dict_compression(
        metadata.get(FIELD_DICT_COMPRESSION_KEY).map(String::as_str),
    )?;
    let encoded_field_dict = required_value(metadata, FIELD_DICT_KEY)?;
    let encoded_len = encoded_field_dict
        .chars()
        .take(MAX_ENCODED_FIELD_DICT_SIZE + 1)
        .count();
    if encoded_len > MAX_ENCODED_FIELD_DICT_SIZE {
        return Err(Error::DataInvalid {
            message: format!(
                "malformed shredding metadata: encoded field dictionary exceeds maximum {MAX_ENCODED_FIELD_DICT_SIZE}"
            ),
            source: None,
        });
    }
    let field_dict_bytes = decompress_dict(
        &string_to_bytes(encoded_field_dict)?,
        original_len,
        compression,
    )?;
    if field_dict_bytes.len() != original_len {
        return Err(Error::DataInvalid {
            message: format!(
                "malformed shredding metadata: decompressed field dictionary size mismatch: expected {original_len}, got {}",
                field_dict_bytes.len()
            ),
            source: None,
        });
    }
    let name_to_id: BTreeMap<String, i32> =
        serde_json::from_slice(&field_dict_bytes).map_err(|e| Error::DataInvalid {
            message: format!("malformed shredding metadata: {e}"),
            source: Some(Box::new(e)),
        })?;
    let field_to_columns_raw: BTreeMap<String, Vec<usize>> =
        serde_json::from_str(required_value(metadata, FIELD_COLUMNS_KEY)?).map_err(|e| {
            Error::DataInvalid {
                message: format!("malformed shredding metadata: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
    let mut field_to_columns = BTreeMap::new();
    for (key, columns) in field_to_columns_raw {
        let field_id: i32 = key.parse().map_err(|e| Error::DataInvalid {
            message: format!("malformed shredding metadata: bad field id '{key}': {e}"),
            source: None,
        })?;
        field_to_columns.insert(field_id, columns);
    }
    let overflow_set: BTreeSet<i32> =
        serde_json::from_str(required_value(metadata, OVERFLOW_SET_KEY)?).map_err(|e| {
            Error::DataInvalid {
                message: format!("malformed shredding metadata: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

    Ok(MapSharedShreddingFieldMeta {
        name_to_id,
        field_to_columns,
        overflow_set,
        num_columns,
        max_row_width,
    })
}

fn required_value<'a>(metadata: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| Error::DataInvalid {
            message: format!("missing shredding metadata key: {key}"),
            source: None,
        })
}

fn required_int(metadata: &HashMap<String, String>, key: &str) -> Result<i32> {
    required_value(metadata, key)?
        .parse()
        .map_err(|e| Error::DataInvalid {
            message: format!("malformed shredding metadata value for key: {key}: {e}"),
            source: None,
        })
}

fn required_bounded_usize(
    metadata: &HashMap<String, String>,
    key: &str,
    maximum: usize,
) -> Result<usize> {
    let signed = required_int(metadata, key)?;
    let value = usize::try_from(signed).map_err(|_| Error::DataInvalid {
        message: format!("malformed shredding metadata value for key: {key}: must be non-negative"),
        source: None,
    })?;
    if value > maximum {
        return Err(Error::DataInvalid {
            message: format!(
                "malformed shredding metadata value for key: {key}: {value} exceeds maximum {maximum}"
            ),
            source: None,
        });
    }
    Ok(value)
}

/// ISO-8859-1 byte -> char mapping, mirroring Java's `bytesToString`.
fn bytes_to_string(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

/// ISO-8859-1 char -> byte mapping, mirroring Java's `stringToBytes`.
fn string_to_bytes(string: &str) -> Result<Vec<u8>> {
    string
        .chars()
        .map(|c| {
            let code = c as u32;
            if code <= 0xFF {
                Ok(code as u8)
            } else {
                Err(Error::DataInvalid {
                    message: format!(
                        "malformed shredding metadata: character U+{code:04X} is not ISO-8859-1"
                    ),
                    source: None,
                })
            }
        })
        .collect()
}

fn compress_dict(input: &[u8], compression: &str) -> Result<Vec<u8>> {
    match compression {
        "none" => Ok(input.to_vec()),
        "zstd" => zstd::bulk::compress(input, 1).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to compress shared-shredding field dict: {e}"),
            source: Some(Box::new(e)),
        }),
        "lz4" => Ok(lz4_compress(input)),
        other => Err(Error::DataInvalid {
            message: format!("Unsupported shared-shredding field dict compression: {other}"),
            source: None,
        }),
    }
}

fn decompress_dict(input: &[u8], original_len: usize, compression: &str) -> Result<Vec<u8>> {
    match compression {
        "none" => Ok(input.to_vec()),
        "zstd" => zstd::bulk::decompress(input, original_len).map_err(|e| Error::DataInvalid {
            message: format!("Failed to decompress shared-shredding field dict: {e}"),
            source: Some(Box::new(e)),
        }),
        "lz4" => lz4_decompress(input, original_len),
        other => Err(Error::DataInvalid {
            message: format!("Unsupported shared-shredding field dict compression: {other}"),
            source: None,
        }),
    }
}

/// LZ4 block compression with the 8-byte header used by Java's
/// `Lz4BlockCompressor`: `[compressed_len: i32 LE][original_len: i32 LE]`
/// followed by the raw LZ4 block (not the LZ4 frame format).
fn lz4_compress(input: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::block::compress(input);
    let mut out = Vec::with_capacity(8 + compressed.len());
    out.extend_from_slice(&(compressed.len() as i32).to_le_bytes());
    out.extend_from_slice(&(input.len() as i32).to_le_bytes());
    out.extend_from_slice(&compressed);
    out
}

fn lz4_decompress(input: &[u8], original_len: usize) -> Result<Vec<u8>> {
    if input.len() < 8 {
        return Err(Error::DataInvalid {
            message: "Input is corrupted: LZ4 block too small for header".to_string(),
            source: None,
        });
    }
    let compressed_len = i32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
    let declared_original_len = i32::from_le_bytes(input[4..8].try_into().unwrap()) as usize;
    if declared_original_len != original_len || input.len() - 8 < compressed_len {
        return Err(Error::DataInvalid {
            message: "Input is corrupted: LZ4 block header mismatch".to_string(),
            source: None,
        });
    }
    lz4_flex::block::decompress(&input[8..8 + compressed_len], original_len).map_err(|e| {
        Error::DataInvalid {
            message: format!("Input is corrupted: {e}"),
            source: Some(Box::new(e)),
        }
    })
}

// ---------------------------------------------------------------------------
// Write plan (mirroring Java's MapSharedShreddingWritePlan[Factory])
// ---------------------------------------------------------------------------

/// [`ShreddingWritePlan`] for the MAP shared-shredding layout.
///
/// Owns the file-local field dictionary and column allocator for every
/// shredded field; both accumulate across all batches written to the file and
/// are serialized into footer metadata at close time.
pub(crate) struct MapShreddingWritePlan {
    logical_fields: Vec<DataField>,
    physical_fields: Vec<DataField>,
    physical_schema: arrow_schema::SchemaRef,
    contexts: HashMap<usize, MapWriteContext>,
}

struct MapWriteContext {
    num_columns: usize,
    dict: FieldDict,
    allocator: ColumnAllocator,
    /// Arrow fields of the physical struct, in layout order.
    struct_fields: Fields,
}

impl MapShreddingWritePlan {
    /// Create a plan, inferring the column count per field from the sampled
    /// rows (mirroring Java's `MapSharedShreddingWritePlanFactory.createWritePlan`).
    pub(crate) fn infer(
        logical_fields: &[DataField],
        configs: &[MapShreddingFieldConfig],
        sample_batches: &[RecordBatch],
    ) -> Result<Self> {
        let config_by_index: HashMap<usize, &MapShreddingFieldConfig> = configs
            .iter()
            .map(|config| (config.field_index, config))
            .collect();

        let mut contexts = HashMap::new();
        let mut physical_fields = Vec::with_capacity(logical_fields.len());
        for (index, field) in logical_fields.iter().enumerate() {
            let Some(config) = config_by_index.get(&index) else {
                physical_fields.push(field.clone());
                continue;
            };
            let DataType::Map(map_type) = field.data_type() else {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Shared-shredding field '{}' must be MAP, got {:?}",
                        field.name(),
                        field.data_type()
                    ),
                    source: None,
                });
            };
            let num_columns = infer_num_columns(config, sample_batches)?;
            let physical_type =
                build_physical_struct_type(map_type.value_type(), num_columns, true)?
                    .copy_with_nullable(field.data_type().is_nullable())?;
            let struct_fields = match paimon_type_to_arrow(&physical_type)? {
                ArrowDataType::Struct(fields) => fields,
                other => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Shared-shredding physical type must be Struct, got {other:?}"
                        ),
                        source: None,
                    })
                }
            };
            physical_fields.push(
                DataField::new(field.id(), field.name().to_string(), physical_type)
                    .with_description(field.description().map(ToString::to_string)),
            );
            contexts.insert(
                index,
                MapWriteContext {
                    num_columns,
                    dict: FieldDict::new(),
                    allocator: ColumnAllocator::new(num_columns),
                    struct_fields,
                },
            );
        }

        let physical_schema = build_target_arrow_schema(&physical_fields)?;
        Ok(Self {
            logical_fields: logical_fields.to_vec(),
            physical_fields,
            physical_schema,
            contexts,
        })
    }
}

/// Infer the physical column count for one field, mirroring Java:
/// `maxColumns` when no rows were sampled, otherwise
/// `max(1, min(maxRowWidth, maxColumns))` over the first
/// [`MAP_SHREDDING_INFER_BUFFER_ROW_COUNT`] rows.
fn infer_num_columns(
    config: &MapShreddingFieldConfig,
    sample_batches: &[RecordBatch],
) -> Result<usize> {
    let mut max_row_width = 0usize;
    let mut sampled = 0usize;
    'outer: for batch in sample_batches {
        let map_array = batch
            .column(config.field_index)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Shared-shredding MAP column '{}' must be MapArray",
                    config.field_name
                ),
                source: None,
            })?;
        for row in 0..map_array.len() {
            if sampled >= MAP_SHREDDING_INFER_BUFFER_ROW_COUNT {
                break 'outer;
            }
            if !map_array.is_null(row) {
                max_row_width = max_row_width.max(map_array.value_length(row) as usize);
            }
            sampled += 1;
        }
    }
    if sampled == 0 {
        return Ok(config.max_columns);
    }
    Ok(max_row_width.clamp(1, config.max_columns))
}

impl ShreddingWritePlan for MapShreddingWritePlan {
    fn logical_fields(&self) -> &[DataField] {
        &self.logical_fields
    }

    fn physical_fields(&self) -> &[DataField] {
        &self.physical_fields
    }

    fn to_physical_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (index, field) in self.logical_fields.iter().enumerate() {
            match self.contexts.get_mut(&index) {
                Some(ctx) => columns.push(map_array_to_physical(
                    batch.column(index).as_ref(),
                    ctx,
                    field.name(),
                )?),
                None => columns.push(batch.column(index).clone()),
            }
        }
        RecordBatch::try_new(self.physical_schema.clone(), columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build shared-shredding RecordBatch: {e}"),
                source: Some(Box::new(e)),
            }
        })
    }

    fn field_metadata(&self, compression: Option<&str>) -> Result<FieldMetadata> {
        let mut metadata = HashMap::new();
        for (&index, ctx) in &self.contexts {
            let field_meta = MapSharedShreddingFieldMeta {
                name_to_id: ctx.dict.name_to_id.clone(),
                field_to_columns: ctx
                    .allocator
                    .field_to_columns
                    .iter()
                    .map(|(&field_id, columns)| (field_id, columns.iter().copied().collect()))
                    .collect(),
                overflow_set: ctx.allocator.overflow_field_set.clone(),
                num_columns: ctx.allocator.num_columns,
                max_row_width: ctx.allocator.max_row_width,
            };
            metadata.insert(
                self.logical_fields[index].name().to_string(),
                serialize_metadata(&field_meta, compression)?,
            );
        }
        Ok(metadata)
    }
}

/// Convert one logical MAP column into the physical struct layout, mirroring
/// Java's `MapSharedShreddingRowConverter.convertMap`.
///
/// The conversion is type-agnostic: because the allocator assigns columns by
/// row order, physical column `i` holds the `i`-th map entry of each row, so
/// column values are gathered with a single `take` over the flattened map
/// values.
fn map_array_to_physical(
    array: &dyn Array,
    ctx: &mut MapWriteContext,
    column_name: &str,
) -> Result<ArrayRef> {
    let map_array =
        array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!("Shared-shredding MAP column '{column_name}' must be MapArray"),
                source: None,
            })?;
    let num_rows = map_array.len();
    let k = ctx.num_columns;
    let keys = map_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Shared-shredding MAP column '{column_name}' keys must be StringArray"
            ),
            source: None,
        })?;
    let values = map_array.values();
    let offsets = map_array.offsets().clone();

    let mut struct_validity = Vec::with_capacity(num_rows);
    let mut mapping_values: Vec<Option<i32>> = Vec::with_capacity(num_rows * k);
    let mut mapping_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut mapping_validity = Vec::with_capacity(num_rows);
    let mut column_indices: Vec<Vec<Option<u32>>> =
        (0..k).map(|_| Vec::with_capacity(num_rows)).collect();
    let mut overflow_keys: Vec<i32> = Vec::new();
    let mut overflow_value_indices: Vec<Option<u32>> = Vec::new();
    let mut overflow_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut overflow_validity = Vec::with_capacity(num_rows);

    mapping_offsets.push(0);
    overflow_offsets.push(0);
    for row in 0..num_rows {
        if map_array.is_null(row) {
            struct_validity.push(false);
            mapping_validity.push(false);
            mapping_offsets.push(*mapping_offsets.last().unwrap());
            overflow_validity.push(false);
            overflow_offsets.push(*overflow_offsets.last().unwrap());
            for indices in column_indices.iter_mut() {
                indices.push(None);
            }
            continue;
        }
        struct_validity.push(true);

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut field_ids = Vec::with_capacity(end - start);
        for j in start..end {
            if keys.is_null(j) {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Shared-shredding MAP keys cannot be null for field: {column_name}"
                    ),
                    source: None,
                });
            }
            field_ids.push(ctx.dict.get_or_assign(keys.value(j)));
        }
        let allocation = ctx.allocator.allocate_row(&field_ids);

        // __field_mapping
        mapping_validity.push(true);
        mapping_values.extend(allocation.col_to_field.iter().map(|&id| Some(id)));
        mapping_offsets.push(*mapping_offsets.last().unwrap() + k as i32);

        // __col_i holds the i-th entry's value (allocation is by row order).
        for (i, indices) in column_indices.iter_mut().enumerate() {
            indices.push(if allocation.col_to_field[i] >= 0 {
                Some((start + i) as u32)
            } else {
                None
            });
        }

        // __overflow (null when the row has no overflow entries, mirroring Java).
        if allocation.overflow_fields.is_empty() {
            overflow_validity.push(false);
            overflow_offsets.push(*overflow_offsets.last().unwrap());
        } else {
            overflow_validity.push(true);
            for (j, &field_id) in allocation.overflow_fields.iter().enumerate() {
                overflow_keys.push(field_id);
                overflow_value_indices.push(Some((start + k + j) as u32));
            }
            overflow_offsets
                .push(*overflow_offsets.last().unwrap() + allocation.overflow_fields.len() as i32);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(k + 2);

    // __field_mapping, typed exactly as declared in the physical struct.
    let ArrowDataType::List(mapping_element_field) = ctx.struct_fields[0].data_type() else {
        return Err(Error::UnexpectedError {
            message: "Shared-shredding __field_mapping must be List".to_string(),
            source: None,
        });
    };
    columns.push(Arc::new(ListArray::new(
        mapping_element_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(mapping_offsets)),
        Arc::new(Int32Array::from(mapping_values)),
        Some(NullBuffer::from(mapping_validity)),
    )));

    // __col_0..__col_{K-1}
    for indices in column_indices {
        columns.push(
            take(values.as_ref(), &UInt32Array::from(indices), None).map_err(|e| {
                Error::UnexpectedError {
                    message: format!("Failed to gather shared-shredding column values: {e}"),
                    source: Some(Box::new(e)),
                }
            })?,
        );
    }

    // __overflow, typed exactly as declared in the physical struct.
    let ArrowDataType::Map(overflow_entries_field, _) = ctx.struct_fields[k + 1].data_type() else {
        return Err(Error::UnexpectedError {
            message: "Shared-shredding __overflow must be Map".to_string(),
            source: None,
        });
    };
    let ArrowDataType::Struct(overflow_entry_fields) = overflow_entries_field.data_type() else {
        return Err(Error::UnexpectedError {
            message: "Shared-shredding __overflow entries must be Struct".to_string(),
            source: None,
        });
    };
    let overflow_values = take(
        values.as_ref(),
        &UInt32Array::from(overflow_value_indices),
        None,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to gather shared-shredding overflow values: {e}"),
        source: Some(Box::new(e)),
    })?;
    let overflow_entries = StructArray::try_new(
        overflow_entry_fields.clone(),
        vec![
            Arc::new(Int32Array::from(overflow_keys)) as ArrayRef,
            overflow_values,
        ],
        None,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build shared-shredding overflow entries: {e}"),
        source: Some(Box::new(e)),
    })?;
    columns.push(Arc::new(MapArray::new(
        overflow_entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(overflow_offsets)),
        overflow_entries,
        Some(NullBuffer::from(overflow_validity)),
        false,
    )));

    Ok(Arc::new(
        StructArray::try_new(
            ctx.struct_fields.clone(),
            columns,
            Some(NullBuffer::from(struct_validity)),
        )
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build shared-shredding physical struct: {e}"),
            source: Some(Box::new(e)),
        })?,
    ))
}

// ---------------------------------------------------------------------------
// Read plan (mirroring Java's MapSharedShreddingReadPlan[Factory])
// ---------------------------------------------------------------------------

/// [`ShreddingReadPlan`] that rebuilds logical MAP values from
/// shared-shredding physical ROW values.
pub(crate) struct MapShreddingReadPlan {
    logical_fields: Vec<DataField>,
    /// Physical fields decoded from the file; part of the mirrored
    /// [`ShreddingReadPlan`] API surface.
    #[allow(dead_code)]
    physical_fields: Vec<DataField>,
    logical_schema: arrow_schema::SchemaRef,
    contexts: HashMap<usize, MapReadContext>,
}

struct MapReadContext {
    num_columns: usize,
    name_by_id: HashMap<i32, String>,
}

impl MapShreddingReadPlan {
    /// Create a read plan from the logical read fields and the file's Arrow
    /// schema (which carries the per-field shredding metadata committed at
    /// write time). Returns `None` when no read field is shared-shredded,
    /// mirroring Java's `MapSharedShreddingReadPlanFactory`.
    pub(crate) fn create(
        logical_fields: &[DataField],
        file_schema: &ArrowSchema,
    ) -> Result<Option<Self>> {
        let mut contexts = HashMap::new();
        let mut physical_fields = Vec::with_capacity(logical_fields.len());
        let mut converted = false;
        for (index, field) in logical_fields.iter().enumerate() {
            let arrow_field = file_schema.field_with_name(field.name()).ok();
            let field_meta = arrow_field
                .map(|arrow_field| arrow_field.metadata())
                .filter(|metadata| has_shredding_metadata(Some(metadata)))
                .map(deserialize_metadata)
                .transpose()?;
            match (field_meta, field.data_type()) {
                (Some(field_meta), DataType::Map(map_type)) => {
                    let arrow_field = arrow_field.expect("metadata came from this Arrow field");
                    validate_physical_struct_children(arrow_field, &field_meta)?;
                    let physical_type = build_physical_struct_type(
                        map_type.value_type(),
                        field_meta.num_columns,
                        !field_meta.overflow_set.is_empty(),
                    )?
                    .copy_with_nullable(field.data_type().is_nullable())?;
                    physical_fields.push(
                        DataField::new(field.id(), field.name().to_string(), physical_type)
                            .with_description(field.description().map(ToString::to_string)),
                    );
                    contexts.insert(
                        index,
                        MapReadContext {
                            num_columns: field_meta.num_columns,
                            name_by_id: field_meta
                                .name_to_id
                                .iter()
                                .map(|(name, &id)| (id, name.clone()))
                                .collect(),
                        },
                    );
                    converted = true;
                }
                _ => physical_fields.push(field.clone()),
            }
        }
        if !converted {
            return Ok(None);
        }
        Ok(Some(Self {
            logical_fields: logical_fields.to_vec(),
            physical_fields,
            logical_schema: build_target_arrow_schema(logical_fields)?,
            contexts,
        }))
    }
}

fn validate_physical_struct_children(
    arrow_field: &arrow_schema::Field,
    field_meta: &MapSharedShreddingFieldMeta,
) -> Result<()> {
    let children_without_overflow =
        field_meta
            .num_columns
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Shared-shredding physical column '{}' child count overflows usize",
                    arrow_field.name()
                ),
                source: None,
            })?;
    let children_with_overflow =
        children_without_overflow
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Shared-shredding physical column '{}' child count overflows usize",
                    arrow_field.name()
                ),
                source: None,
            })?;
    let ArrowDataType::Struct(children) = arrow_field.data_type() else {
        return Err(Error::DataInvalid {
            message: format!(
                "Shared-shredding physical column '{}' must be Struct, got {:?}",
                arrow_field.name(),
                arrow_field.data_type()
            ),
            source: None,
        });
    };
    let child_count_matches = children.len() == children_with_overflow
        || (field_meta.overflow_set.is_empty() && children.len() == children_without_overflow);
    if !child_count_matches {
        return Err(Error::DataInvalid {
            message: format!(
                "Shared-shredding physical column '{}' has {} children, metadata requires {}{}",
                arrow_field.name(),
                children.len(),
                if field_meta.overflow_set.is_empty() {
                    format!("{children_without_overflow} or ")
                } else {
                    String::new()
                },
                children_with_overflow
            ),
            source: None,
        });
    }
    Ok(())
}

#[cfg(test)]
mod metadata_security_tests {
    use super::*;
    use crate::spec::VarCharType;
    use arrow_schema::Field as ArrowField;

    fn metadata() -> HashMap<String, String> {
        serialize_metadata(
            &MapSharedShreddingFieldMeta {
                name_to_id: BTreeMap::new(),
                field_to_columns: BTreeMap::new(),
                overflow_set: BTreeSet::new(),
                num_columns: 1,
                max_row_width: 1,
            },
            Some("none"),
        )
        .unwrap()
    }

    fn logical_fields() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "metrics".to_string(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        )]
    }

    #[test]
    fn test_rejects_invalid_field_dict_original_size_before_decompression() {
        for value in ["-1".to_string(), (MAX_FIELD_DICT_SIZE + 1).to_string()] {
            let mut malformed = metadata();
            malformed.insert(FIELD_DICT_ORIGINAL_SIZE_KEY.to_string(), value);
            malformed.insert(FIELD_DICT_COMPRESSION_KEY.to_string(), "zstd".to_string());
            malformed.insert(FIELD_DICT_KEY.to_string(), "not-zstd".to_string());
            let error = deserialize_metadata(&malformed).unwrap_err();
            assert!(
                error.to_string().contains("field dictionary original size"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn test_rejects_decompressed_field_dict_size_mismatch() {
        for compression in ["none", "zstd"] {
            let encoded = compress_dict(b"{}", compression).unwrap();
            let mut malformed = metadata();
            malformed.insert(
                FIELD_DICT_COMPRESSION_KEY.to_string(),
                compression.to_string(),
            );
            malformed.insert(FIELD_DICT_KEY.to_string(), bytes_to_string(&encoded));
            malformed.insert(FIELD_DICT_ORIGINAL_SIZE_KEY.to_string(), "3".to_string());
            let error = deserialize_metadata(&malformed).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("decompressed field dictionary size mismatch: expected 3, got 2"),
                "unexpected error for {compression}: {error}"
            );
        }
    }

    #[test]
    fn test_rejects_invalid_shared_shredding_dimensions() {
        for key in [NUM_COLUMNS_KEY, MAX_ROW_WIDTH_KEY] {
            let mut negative = metadata();
            negative.insert(key.to_string(), "-1".to_string());
            let error = deserialize_metadata(&negative).unwrap_err();
            assert!(
                error.to_string().contains("must be non-negative"),
                "unexpected error for {key}: {error}"
            );
        }

        for (key, maximum) in [
            (NUM_COLUMNS_KEY, MAX_SHARED_SHREDDING_NUM_COLUMNS),
            (MAX_ROW_WIDTH_KEY, MAX_SHARED_SHREDDING_ROW_WIDTH),
        ] {
            let mut oversized = metadata();
            oversized.insert(key.to_string(), (maximum + 1).to_string());
            let error = deserialize_metadata(&oversized).unwrap_err();
            assert!(
                error.to_string().contains("exceeds maximum"),
                "unexpected error for {key}: {error}"
            );
        }
    }

    #[test]
    fn test_rejects_physical_child_count_mismatch_before_schema_build() {
        let logical_fields = logical_fields();
        let mut metadata = metadata();
        metadata.insert(NUM_COLUMNS_KEY.to_string(), "2".to_string());
        metadata.insert(MAX_ROW_WIDTH_KEY.to_string(), "2".to_string());
        let physical_type =
            build_physical_struct_type(&DataType::Int(IntType::new()), 1, false).unwrap();
        let file_schema = ArrowSchema::new(vec![ArrowField::new(
            "metrics",
            paimon_type_to_arrow(&physical_type).unwrap(),
            true,
        )
        .with_metadata(metadata)]);

        let error = match MapShreddingReadPlan::create(&logical_fields, &file_schema) {
            Err(error) => error,
            Ok(_) => panic!("mismatched physical child count must be rejected"),
        };
        assert!(
            error
                .to_string()
                .contains("has 2 children, metadata requires 3 or 4"),
            "unexpected error: {error}"
        );
    }
}

impl ShreddingReadPlan for MapShreddingReadPlan {
    fn logical_fields(&self) -> &[DataField] {
        &self.logical_fields
    }

    fn physical_fields(&self) -> &[DataField] {
        &self.physical_fields
    }

    fn assemble_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut changed = false;
        let mut columns = Vec::with_capacity(batch.num_columns());
        let mut output_fields = Vec::with_capacity(batch.num_columns());

        for (idx, arrow_field) in schema.fields().iter().enumerate() {
            let column = batch.column(idx);
            let logical_idx = self
                .logical_fields
                .iter()
                .position(|field| field.name() == arrow_field.name());

            if let Some(field_idx) = logical_idx {
                if let Some(ctx) = self.contexts.get(&field_idx) {
                    let DataType::Map(map_type) = self.logical_fields[field_idx].data_type() else {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Shared-shredding field '{}' must be MAP",
                                arrow_field.name()
                            ),
                            source: None,
                        });
                    };
                    let struct_array =
                        column
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .ok_or_else(|| Error::DataInvalid {
                                message: format!(
                                    "Shared-shredding physical column '{}' must be StructArray",
                                    arrow_field.name()
                                ),
                                source: None,
                            })?;
                    columns.push(struct_array_to_logical_map(
                        struct_array,
                        ctx,
                        map_type,
                        arrow_field.name(),
                    )?);
                    output_fields.push(self.logical_schema.field(field_idx).clone());
                    changed = true;
                    continue;
                }
            }

            columns.push(column.clone());
            output_fields.push(arrow_field.as_ref().clone());
        }

        if !changed {
            return Ok(batch.clone());
        }

        RecordBatch::try_new(Arc::new(ArrowSchema::new(output_fields)), columns).map_err(|e| {
            Error::UnexpectedError {
                message: format!("Failed to build assembled shared-shredding RecordBatch: {e}"),
                source: Some(Box::new(e)),
            }
        })
    }
}

/// Rebuild the logical MAP column from the physical struct, mirroring Java's
/// `MapSharedShreddingReadPlan.materializeLogicalMapVector`.
///
/// Type-agnostic: only mapped direct-column and recognized overflow values are
/// selected. Each selected value is normalized to the logical value type so
/// nested Arrow field metadata from Java-written files does not prevent
/// assembly.
fn struct_array_to_logical_map(
    array: &StructArray,
    ctx: &MapReadContext,
    map_type: &MapType,
    column_name: &str,
) -> Result<ArrayRef> {
    let num_rows = array.len();
    let k = ctx.num_columns;
    if array.num_columns() < k + 1 {
        return Err(Error::DataInvalid {
            message: format!(
                "Shared-shredding physical column '{column_name}' has {} children, expected at least {}",
                array.num_columns(),
                k + 1
            ),
            source: None,
        });
    }

    let field_mapping = array
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Shared-shredding __field_mapping of '{column_name}' must be ListArray"
            ),
            source: None,
        })?;
    let mapping_values = field_mapping
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Shared-shredding __field_mapping values of '{column_name}' must be Int32Array"
            ),
            source: None,
        })?;
    let mapping_offsets = field_mapping.offsets().clone();

    // Build the map with the exact Arrow type of the logical map field. The
    // value type is also the normalization target for metadata-distinct nested
    // physical sources.
    let ArrowDataType::Map(entries_field, ordered) =
        paimon_type_to_arrow(&DataType::Map(map_type.clone()))?
    else {
        return Err(Error::UnexpectedError {
            message: "Logical MAP type must convert to Arrow Map".to_string(),
            source: None,
        });
    };
    let ArrowDataType::Struct(entry_fields) = entries_field.data_type().clone() else {
        return Err(Error::UnexpectedError {
            message: "Logical MAP entries must be Struct".to_string(),
            source: None,
        });
    };
    let logical_value_type = entry_fields[1].data_type();

    let has_overflow_column = array.num_columns() > k + 1;
    let overflow = if has_overflow_column {
        Some(
            array
                .column(k + 1)
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Shared-shredding __overflow of '{column_name}' must be MapArray"
                    ),
                    source: None,
                })?,
        )
    } else {
        None
    };

    let normalize_source = |value: &dyn Array| {
        arrow_cast::cast(value, logical_value_type).map_err(|e| Error::UnexpectedError {
            message: format!(
                "Failed to normalize shared-shredding value source for '{column_name}': {e}"
            ),
            source: Some(Box::new(e)),
        })
    };

    // Normalize each physical source once. For metadata-only differences this
    // rebuilds nested Arrow wrappers while reusing their value buffers. The
    // final interleave then copies only entries referenced by the mapping.
    let mut value_sources = Vec::with_capacity(k + usize::from(overflow.is_some()));
    for i in 0..k {
        value_sources.push(normalize_source(array.column(i + 1).as_ref())?);
    }
    let overflow_source_index = if let Some(overflow) = overflow {
        let index = value_sources.len();
        value_sources.push(normalize_source(overflow.values().as_ref())?);
        Some(index)
    } else {
        None
    };

    let mut key_builder = arrow_array::builder::StringBuilder::new();
    let mut value_indices: Vec<(usize, usize)> = Vec::new();
    let mut map_offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut map_validity = Vec::with_capacity(num_rows);
    map_offsets.push(0);

    for row in 0..num_rows {
        if array.is_null(row) {
            map_validity.push(false);
            map_offsets.push(*map_offsets.last().unwrap());
            continue;
        }
        map_validity.push(true);
        let mut count = 0i32;

        let fm_start = mapping_offsets[row] as usize;
        let fm_end = mapping_offsets[row + 1] as usize;
        if fm_end - fm_start != k {
            return Err(Error::DataInvalid {
                message: format!(
                    "Shared-shredding field mapping size {} does not match metadata num columns {}.",
                    fm_end - fm_start,
                    k
                ),
                source: None,
            });
        }
        for i in 0..k {
            let field_id = if mapping_values.is_null(fm_start + i) {
                -1
            } else {
                mapping_values.value(fm_start + i)
            };
            if field_id < 0 {
                continue;
            }
            let Some(name) = ctx.name_by_id.get(&field_id) else {
                continue;
            };
            key_builder.append_value(name);
            value_indices.push((i, row));
            count += 1;
        }

        if let Some(overflow) = overflow {
            if !overflow.is_null(row) {
                let overflow_keys = overflow
                    .keys()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| Error::DataInvalid {
                        message: format!(
                            "Shared-shredding __overflow keys of '{column_name}' must be Int32Array"
                        ),
                        source: None,
                    })?;
                let ov_start = overflow.offsets()[row] as usize;
                let ov_end = overflow.offsets()[row + 1] as usize;
                for j in ov_start..ov_end {
                    let field_id = overflow_keys.value(j);
                    let Some(name) = ctx.name_by_id.get(&field_id) else {
                        continue;
                    };
                    key_builder.append_value(name);
                    value_indices.push((overflow_source_index.expect("overflow source exists"), j));
                    count += 1;
                }
            }
        }
        map_offsets.push(*map_offsets.last().unwrap() + count);
    }

    let value_source_refs: Vec<&dyn Array> =
        value_sources.iter().map(|value| value.as_ref()).collect();
    let values =
        interleave(&value_source_refs, &value_indices).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to gather shared-shredding logical values: {e}"),
            source: Some(Box::new(e)),
        })?;
    let keys = Arc::new(key_builder.finish()) as ArrayRef;

    let entries = StructArray::try_new(entry_fields, vec![keys, values], None).map_err(|e| {
        Error::UnexpectedError {
            message: format!("Failed to build shared-shredding logical map entries: {e}"),
            source: Some(Box::new(e)),
        }
    })?;
    Ok(Arc::new(MapArray::new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(map_offsets)),
        entries,
        Some(NullBuffer::from(map_validity)),
        ordered,
    )))
}

// ---------------------------------------------------------------------------
// Tests (mirroring Java's MapSharedShredding*Test semantics)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::type_complexity)] // test row literals use nested Option<Vec<(&str, Option<i64>)>>
mod tests {
    use super::*;
    use crate::spec::{BigIntType, DoubleType, VarCharType};
    use arrow_array::Int64Array;
    use arrow_schema::Field as ArrowField;

    fn string_type() -> DataType {
        DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap())
    }

    fn bigint_type() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    fn map_field(id: i32, name: &str, value_type: DataType) -> DataField {
        DataField::new(
            id,
            name.to_string(),
            DataType::Map(MapType::new(string_type(), value_type)),
        )
    }

    fn logical_batch(fields: &[DataField], columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(build_target_arrow_schema(fields).unwrap(), columns).unwrap()
    }

    /// Build a MapArray from per-row entries, typed exactly like
    /// `paimon_type_to_arrow` for `MAP<STRING, BIGINT>`.
    fn build_map_array(rows: &[Option<Vec<(&str, Option<i64>)>>]) -> MapArray {
        let map_type = DataType::Map(MapType::new(string_type(), bigint_type()));
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(&map_type).unwrap()
        else {
            panic!("map type must convert to Arrow Map")
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type().clone() else {
            panic!("map entries must be Struct")
        };

        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = vec![0i32];
        let mut validity = Vec::new();
        for row in rows {
            match row {
                None => {
                    validity.push(false);
                    offsets.push(*offsets.last().unwrap());
                }
                Some(entries) => {
                    validity.push(true);
                    for (key, value) in entries {
                        keys.push(*key);
                        values.push(*value);
                    }
                    offsets.push(*offsets.last().unwrap() + entries.len() as i32);
                }
            }
        }
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(values)),
            ],
            None,
        )
        .unwrap();
        MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            Some(NullBuffer::from(validity)),
            ordered,
        )
    }

    fn build_non_nullable_map_array(rows: &[Vec<(&str, i64)>]) -> MapArray {
        let value_type = bigint_type().copy_with_nullable(false).unwrap();
        let map_type = DataType::Map(MapType::new(string_type(), value_type));
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(&map_type).unwrap()
        else {
            panic!("map type must convert to Arrow Map")
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type().clone() else {
            panic!("map entries must be Struct")
        };

        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = vec![0i32];
        for entries in rows {
            for (key, value) in entries {
                keys.push(*key);
                values.push(*value);
            }
            offsets.push(*offsets.last().unwrap() + entries.len() as i32);
        }
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(values)),
            ],
            None,
        )
        .unwrap();
        MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            None,
            ordered,
        )
    }

    fn map_array_to_rows(map: &MapArray) -> Vec<Option<Vec<(String, Option<i64>)>>> {
        let keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let values = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
        (0..map.len())
            .map(|row| {
                if map.is_null(row) {
                    return None;
                }
                let start = map.offsets()[row] as usize;
                let end = map.offsets()[row + 1] as usize;
                Some(
                    (start..end)
                        .map(|j| {
                            let value = if values.is_null(j) {
                                None
                            } else {
                                Some(values.value(j))
                            };
                            (keys.value(j).to_string(), value)
                        })
                        .collect(),
                )
            })
            .collect()
    }

    fn physical_struct_fields(
        value_type: &DataType,
        num_columns: usize,
        include_overflow: bool,
    ) -> Fields {
        match paimon_type_to_arrow(
            &build_physical_struct_type(value_type, num_columns, include_overflow).unwrap(),
        )
        .unwrap()
        {
            ArrowDataType::Struct(fields) => fields,
            other => panic!("physical type must be Struct, got {other:?}"),
        }
    }

    fn mapping_list(element_field: &ArrowField, ids: Vec<i32>) -> ListArray {
        ListArray::new(
            Arc::new(element_field.clone()),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, ids.len() as i32])),
            Arc::new(Int32Array::from(ids)),
            None,
        )
    }

    fn assemble_nested_values(
        value_type: DataType,
        mapping_ids: Vec<i32>,
        columns: Vec<ArrayRef>,
        name_by_id: HashMap<i32, String>,
    ) -> ArrayRef {
        assert_eq!(mapping_ids.len(), columns.len());
        let mapping_element = ArrowField::new("element", ArrowDataType::Int32, true);
        let mapping = mapping_list(&mapping_element, mapping_ids);
        let mut fields = vec![ArrowField::new(
            FIELD_MAPPING_NAME,
            mapping.data_type().clone(),
            true,
        )];
        fields.extend(columns.iter().enumerate().map(|(i, column)| {
            ArrowField::new(physical_column_name(i), column.data_type().clone(), true)
        }));
        let num_columns = columns.len();
        let mut children = vec![Arc::new(mapping) as ArrayRef];
        children.extend(columns);
        let physical = StructArray::try_new(fields.into(), children, None).unwrap();
        struct_array_to_logical_map(
            &physical,
            &MapReadContext {
                num_columns,
                name_by_id,
            },
            &MapType::new(string_type(), value_type),
            "nested",
        )
        .unwrap()
    }

    fn metadata_distinct_list(field_id: &str, values: Vec<i64>) -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(
                ArrowField::new("element", ArrowDataType::Int64, true).with_metadata(
                    HashMap::from([("PARQUET:field_id".to_string(), field_id.to_string())]),
                ),
            ),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32])),
            Arc::new(Int64Array::from(values)),
            None,
        ))
    }

    fn metadata_distinct_map(field_id: &str, key: &str, value: i64) -> ArrayRef {
        let metadata = HashMap::from([("PARQUET:field_id".to_string(), field_id.to_string())]);
        let entry_fields: Fields = vec![
            ArrowField::new("key", ArrowDataType::Utf8, false).with_metadata(metadata.clone()),
            ArrowField::new("value", ArrowDataType::Int64, true).with_metadata(metadata.clone()),
        ]
        .into();
        let entries_field = Arc::new(
            ArrowField::new(
                "entries",
                ArrowDataType::Struct(entry_fields.clone()),
                false,
            )
            .with_metadata(metadata),
        );
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(vec![key])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(value)])),
            ],
            None,
        )
        .unwrap();
        Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            entries,
            None,
            false,
        ))
    }

    #[test]
    fn test_read_java_nested_values_with_distinct_field_metadata() {
        let array_value_type = DataType::Array(ArrayType::new(bigint_type()));
        let assembled = assemble_nested_values(
            array_value_type.clone(),
            vec![0, 1],
            vec![
                metadata_distinct_list("10", vec![10]),
                metadata_distinct_list("11", vec![20, 21]),
            ],
            HashMap::from([(0, "a".to_string()), (1, "b".to_string())]),
        );
        let outer = assembled.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            outer.data_type(),
            &paimon_type_to_arrow(&DataType::Map(MapType::new(
                string_type(),
                array_value_type,
            )))
            .unwrap()
        );
        let lists = outer.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.offsets().as_ref(), &[0, 1, 3]);
        assert_eq!(
            lists
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20, 21]
        );

        let map_value_type = DataType::Map(MapType::new(string_type(), bigint_type()));
        let assembled = assemble_nested_values(
            map_value_type.clone(),
            vec![0, 1],
            vec![
                metadata_distinct_map("20", "x", 30),
                metadata_distinct_map("21", "y", 40),
            ],
            HashMap::from([(0, "a".to_string()), (1, "b".to_string())]),
        );
        let outer = assembled.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            outer.data_type(),
            &paimon_type_to_arrow(&DataType::Map(MapType::new(string_type(), map_value_type,)))
                .unwrap()
        );
        let maps = outer.values().as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(maps.offsets().as_ref(), &[0, 1, 2]);
        assert_eq!(
            maps.keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("x"), Some("y")]
        );
        assert_eq!(
            maps.values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(40)]
        );
    }

    #[test]
    fn test_read_does_not_materialize_unmapped_physical_columns() {
        let array_value_type = DataType::Array(ArrayType::new(bigint_type()));
        let assembled = assemble_nested_values(
            array_value_type,
            vec![0, -1],
            vec![
                metadata_distinct_list("10", vec![10]),
                metadata_distinct_list("11", vec![999]),
            ],
            HashMap::from([(0, "selected".to_string())]),
        );
        let outer = assembled.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(outer.value_length(0), 1);
        let lists = outer.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.offsets().as_ref(), &[0, 1]);
        assert_eq!(
            lists
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10]
        );
    }

    /// Assemble one physical struct column back into a logical map column,
    /// going through metadata serialization exactly like the file path does.
    fn assemble_single(
        field_name: &str,
        physical_type: &DataType,
        logical_fields: &[DataField],
        meta: &MapSharedShreddingFieldMeta,
        struct_array: StructArray,
    ) -> ArrayRef {
        let metadata = serialize_metadata(meta, Some("none")).unwrap();
        let arrow_field = ArrowField::new(
            field_name,
            paimon_type_to_arrow(physical_type).unwrap(),
            true,
        )
        .with_metadata(metadata);
        let file_schema = ArrowSchema::new(vec![arrow_field]);
        let plan = MapShreddingReadPlan::create(logical_fields, &file_schema)
            .unwrap()
            .expect("read plan expected");
        let batch =
            RecordBatch::try_new(Arc::new(file_schema), vec![Arc::new(struct_array)]).unwrap();
        let assembled = plan.assemble_batch(&batch).unwrap();
        assembled.column(0).clone()
    }

    // -- FieldDict / ColumnAllocator (MapSharedShreddingFieldDictTest /
    //    MapSharedShreddingColumnAllocatorTest) --

    #[test]
    fn test_field_dict() {
        let mut dict = FieldDict::new();
        assert_eq!(dict.get_or_assign("a"), 0);
        assert_eq!(dict.get_or_assign("b"), 1);
        assert_eq!(dict.get_or_assign("a"), 0);
        assert_eq!(dict.get_or_assign("c"), 2);
        assert_eq!(dict.name_to_id.len(), 3);
    }

    #[test]
    fn test_column_allocator() {
        // Basic allocation.
        let mut allocator = ColumnAllocator::new(3);
        let allocation = allocator.allocate_row(&[10, 20]);
        assert_eq!(allocation.col_to_field, vec![10, 20, -1]);
        assert!(allocation.overflow_fields.is_empty());

        // Exactly K fields.
        let mut allocator = ColumnAllocator::new(3);
        let allocation = allocator.allocate_row(&[0, 1, 2]);
        assert_eq!(allocation.col_to_field, vec![0, 1, 2]);
        assert!(allocation.overflow_fields.is_empty());

        // Overflow when exceeding K.
        let mut allocator = ColumnAllocator::new(2);
        let allocation = allocator.allocate_row(&[10, 20, 30, 40]);
        assert_eq!(allocation.col_to_field, vec![10, 20]);
        assert_eq!(allocation.overflow_fields, vec![30, 40]);

        // Empty row.
        let mut allocator = ColumnAllocator::new(3);
        let allocation = allocator.allocate_row(&[]);
        assert_eq!(allocation.col_to_field, vec![-1, -1, -1]);
        assert!(allocation.overflow_fields.is_empty());

        // Max row width tracked.
        let mut allocator = ColumnAllocator::new(3);
        allocator.allocate_row(&[1, 2]);
        allocator.allocate_row(&[1, 2, 3, 4, 5]);
        allocator.allocate_row(&[1]);
        assert_eq!(allocator.max_row_width, 5);

        // field_to_columns accumulated.
        let mut allocator = ColumnAllocator::new(3);
        allocator.allocate_row(&[10, 20, 30]);
        allocator.allocate_row(&[20, 40]);
        let columns = |id: i32| {
            allocator
                .field_to_columns
                .get(&id)
                .unwrap()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        };
        assert_eq!(columns(10), vec![0]);
        assert_eq!(columns(20), vec![0, 1]);
        assert_eq!(columns(30), vec![2]);
        assert_eq!(columns(40), vec![1]);

        // overflow_field_set accumulated.
        let mut allocator = ColumnAllocator::new(2);
        allocator.allocate_row(&[1, 2, 3]);
        allocator.allocate_row(&[4, 5, 6, 7]);
        assert_eq!(allocator.overflow_field_set, BTreeSet::from([3, 6, 7]));
    }

    // -- Detection (MapSharedShreddingUtilsTest) --

    #[test]
    fn test_is_shredding_key_map() {
        assert!(is_shredding_key_map(&DataType::Map(MapType::new(
            string_type(),
            DataType::Int(IntType::new())
        ))));
        assert!(is_shredding_key_map(&DataType::Map(MapType::new(
            string_type(),
            DataType::Double(DoubleType::new())
        ))));
        assert!(is_shredding_key_map(&DataType::Map(MapType::new(
            string_type(),
            DataType::Row(RowType::new(vec![
                DataField::new(0, "x".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "y".to_string(), string_type()),
            ]))
        ))));
        assert!(!is_shredding_key_map(&DataType::Map(MapType::new(
            DataType::Int(IntType::new()),
            string_type()
        ))));
        assert!(!is_shredding_key_map(&DataType::Int(IntType::new())));
        assert!(!is_shredding_key_map(&DataType::Array(ArrayType::new(
            string_type()
        ))));
    }

    #[test]
    fn test_detect_map_shredding_fields() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            map_field(1, "tags", string_type()),
            map_field(2, "metrics", bigint_type()),
            DataField::new(
                3,
                "codes".to_string(),
                DataType::Map(MapType::new(DataType::Int(IntType::new()), string_type())),
            ),
        ];
        let options = HashMap::from([
            (
                "fields.tags.map.storage-layout".to_string(),
                "shared-shredding".to_string(),
            ),
            (
                "fields.metrics.map.storage-layout".to_string(),
                "shared-shredding".to_string(),
            ),
            (
                "fields.tags.map.shared-shredding.max-columns".to_string(),
                "128".to_string(),
            ),
            (
                "fields.metrics.map.shared-shredding.max-columns".to_string(),
                "64".to_string(),
            ),
            (
                "fields.codes.map.storage-layout".to_string(),
                "shared-shredding".to_string(),
            ),
        ]);
        let configs = detect_map_shredding_fields(&fields, &options).unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].field_index, 1);
        assert_eq!(configs[0].field_name, "tags");
        assert_eq!(configs[0].max_columns, 128);
        assert_eq!(configs[1].field_index, 2);
        assert_eq!(configs[1].field_name, "metrics");
        assert_eq!(configs[1].max_columns, 64);

        // No options -> nothing detected; default max-columns is 256.
        assert!(detect_map_shredding_fields(&fields, &HashMap::new())
            .unwrap()
            .is_empty());
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "shared-shredding".to_string(),
        )]);
        let configs = detect_map_shredding_fields(&fields, &options).unwrap();
        assert_eq!(
            configs[0].max_columns,
            DEFAULT_MAP_SHARED_SHREDDING_MAX_COLUMNS
        );

        // "default" layout is skipped; values are case-insensitive.
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "default".to_string(),
        )]);
        assert!(detect_map_shredding_fields(&fields, &options)
            .unwrap()
            .is_empty());
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "SHARED-SHREDDING".to_string(),
        )]);
        assert_eq!(
            detect_map_shredding_fields(&fields, &options)
                .unwrap()
                .len(),
            1
        );

        // Invalid layout value -> error.
        let options = HashMap::from([(
            "fields.tags.map.storage-layout".to_string(),
            "bogus".to_string(),
        )]);
        assert!(detect_map_shredding_fields(&fields, &options).is_err());

        // max-columns must be > 0.
        let options = HashMap::from([
            (
                "fields.tags.map.storage-layout".to_string(),
                "shared-shredding".to_string(),
            ),
            (
                "fields.tags.map.shared-shredding.max-columns".to_string(),
                "0".to_string(),
            ),
        ]);
        assert!(detect_map_shredding_fields(&fields, &options).is_err());
    }

    #[test]
    fn test_build_physical_struct_type() {
        let value_type = DataType::Double(DoubleType::new());
        let physical = build_physical_struct_type(&value_type, 2, true).unwrap();
        let DataType::Row(row) = &physical else {
            panic!("physical type must be Row")
        };
        let names: Vec<&str> = row.fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec!["__field_mapping", "__col_0", "__col_1", "__overflow"]
        );
        let ids: Vec<i32> = row.fields().iter().map(|f| f.id()).collect();
        assert_eq!(ids, vec![0, 1, 2, 3]);
        assert_eq!(row.fields()[1].data_type(), &value_type);
        assert_eq!(
            row.fields()[3].data_type(),
            &DataType::Map(MapType::new(
                DataType::Int(IntType::new()),
                value_type.clone()
            ))
        );

        let physical = build_physical_struct_type(&value_type, 2, false).unwrap();
        let DataType::Row(row) = &physical else {
            panic!("physical type must be Row")
        };
        assert_eq!(row.fields().len(), 3);
    }

    #[test]
    fn test_physical_column_name() {
        assert_eq!(physical_column_name(0), "__col_0");
        assert_eq!(physical_column_name(1), "__col_1");
        assert_eq!(physical_column_name(99), "__col_99");
    }

    // -- Metadata serde (MapSharedShreddingUtilsTest) --

    #[test]
    fn test_metadata_roundtrip() {
        let field_meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([("age".to_string(), 0), ("name".to_string(), 1)]),
            field_to_columns: BTreeMap::from([(0, vec![0]), (1, vec![1, 2])]),
            overflow_set: BTreeSet::from([1, 5]),
            num_columns: 3,
            max_row_width: 2,
        };
        let expected_dict = "{\"age\":0,\"name\":1}";
        for compression in ["none", "NONE"] {
            let metadata = serialize_metadata(&field_meta, Some(compression)).unwrap();
            assert!(has_shredding_metadata(Some(&metadata)));
            assert_eq!(
                metadata.get(MAP_STORAGE_LAYOUT_KEY).unwrap(),
                "shared-shredding"
            );
            assert_eq!(metadata.get(VERSION_KEY).unwrap(), "1");
            assert_eq!(metadata.get(FIELD_DICT_COMPRESSION_KEY).unwrap(), "none");
            assert_eq!(metadata.get(NUM_COLUMNS_KEY).unwrap(), "3");
            assert_eq!(metadata.get(MAX_ROW_WIDTH_KEY).unwrap(), "2");
            assert_eq!(metadata.get(FIELD_DICT_KEY).unwrap(), expected_dict);
            assert_eq!(
                metadata.get(FIELD_DICT_ORIGINAL_SIZE_KEY).unwrap(),
                &expected_dict.len().to_string()
            );
            assert_eq!(
                metadata.get(FIELD_COLUMNS_KEY).unwrap(),
                "{\"0\":[0],\"1\":[1,2]}"
            );
            assert_eq!(metadata.get(OVERFLOW_SET_KEY).unwrap(), "[1,5]");
            assert_eq!(deserialize_metadata(&metadata).unwrap(), field_meta);
        }

        // A missing compression key defaults to zstd on deserialize.
        let mut metadata = serialize_metadata(&field_meta, Some("zstd")).unwrap();
        metadata.remove(FIELD_DICT_COMPRESSION_KEY);
        assert_eq!(deserialize_metadata(&metadata).unwrap(), field_meta);
    }

    #[test]
    fn test_metadata_roundtrip_compression() {
        let field_meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([
                ("alpha".to_string(), 0),
                ("beta".to_string(), 1),
                ("gamma".to_string(), 2),
            ]),
            field_to_columns: BTreeMap::from([(0, vec![0, 1, 2]), (1, vec![3]), (2, vec![4, 5])]),
            overflow_set: BTreeSet::from([2]),
            num_columns: 6,
            max_row_width: 3,
        };
        for compression in ["none", "lz4", "zstd"] {
            let metadata = serialize_metadata(&field_meta, Some(compression)).unwrap();
            assert_eq!(
                metadata.get(FIELD_DICT_COMPRESSION_KEY).unwrap(),
                compression
            );
            assert_eq!(deserialize_metadata(&metadata).unwrap(), field_meta);
        }
    }

    #[test]
    fn test_metadata_roundtrip_empty() {
        let field_meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::new(),
            field_to_columns: BTreeMap::new(),
            overflow_set: BTreeSet::new(),
            num_columns: 0,
            max_row_width: 0,
        };
        for compression in ["none", "lz4", "zstd"] {
            let metadata = serialize_metadata(&field_meta, Some(compression)).unwrap();
            assert_eq!(
                metadata.get(FIELD_DICT_COMPRESSION_KEY).unwrap(),
                compression
            );
            assert_eq!(deserialize_metadata(&metadata).unwrap(), field_meta);
        }
    }

    #[test]
    fn test_metadata_rejects_unknown_compression() {
        let field_meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([("age".to_string(), 0)]),
            field_to_columns: BTreeMap::new(),
            overflow_set: BTreeSet::new(),
            num_columns: 1,
            max_row_width: 1,
        };
        let err = serialize_metadata(&field_meta, Some("snappy")).unwrap_err();
        assert!(
            err.to_string()
                .contains("MAP shared-shredding only supports none/lz4/zstd compression"),
            "unexpected error: {err}"
        );

        let mut metadata = serialize_metadata(&field_meta, Some("zstd")).unwrap();
        metadata.insert(FIELD_DICT_COMPRESSION_KEY.to_string(), "snappy".to_string());
        let err = deserialize_metadata(&metadata).unwrap_err();
        assert!(
            err.to_string()
                .contains("MAP shared-shredding only supports none/lz4/zstd compression"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_deserialize_metadata_errors() {
        let missing_layout = HashMap::from([("some_key".to_string(), "some_value".to_string())]);
        let err = deserialize_metadata(&missing_layout).unwrap_err();
        assert!(
            err.to_string()
                .contains("metadata is null or storage layout is not shared-shredding"),
            "unexpected error: {err}"
        );

        let default_layout =
            HashMap::from([(MAP_STORAGE_LAYOUT_KEY.to_string(), "default".to_string())]);
        let err = deserialize_metadata(&default_layout).unwrap_err();
        assert!(
            err.to_string()
                .contains("metadata is null or storage layout is not shared-shredding"),
            "unexpected error: {err}"
        );

        let missing_version = HashMap::from([(
            MAP_STORAGE_LAYOUT_KEY.to_string(),
            MAP_STORAGE_LAYOUT_SHARED_SHREDDING.to_string(),
        )]);
        let err = deserialize_metadata(&missing_version).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing shredding metadata key: paimon.map.shared-shredding.version"),
            "unexpected error: {err}"
        );

        let wrong_version = HashMap::from([
            (
                MAP_STORAGE_LAYOUT_KEY.to_string(),
                MAP_STORAGE_LAYOUT_SHARED_SHREDDING.to_string(),
            ),
            (VERSION_KEY.to_string(), "999".to_string()),
            (FIELD_DICT_ORIGINAL_SIZE_KEY.to_string(), "2".to_string()),
            (FIELD_DICT_KEY.to_string(), "{}".to_string()),
        ]);
        let err = deserialize_metadata(&wrong_version).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported shared-shredding metadata version: 999"),
            "unexpected error: {err}"
        );

        let missing_field_dict = HashMap::from([
            (
                MAP_STORAGE_LAYOUT_KEY.to_string(),
                MAP_STORAGE_LAYOUT_SHARED_SHREDDING.to_string(),
            ),
            (VERSION_KEY.to_string(), "1".to_string()),
            (FIELD_DICT_ORIGINAL_SIZE_KEY.to_string(), "2".to_string()),
            (NUM_COLUMNS_KEY.to_string(), "1".to_string()),
            (MAX_ROW_WIDTH_KEY.to_string(), "1".to_string()),
        ]);
        let err = deserialize_metadata(&missing_field_dict).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing shredding metadata key: paimon.map.shared-shredding.field-dict"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_has_shredding_metadata() {
        assert!(!has_shredding_metadata(None));
        let metadata = HashMap::from([(
            MAP_STORAGE_LAYOUT_KEY.to_string(),
            MAP_STORAGE_LAYOUT_SHARED_SHREDDING.to_string(),
        )]);
        assert!(has_shredding_metadata(Some(&metadata)));
        let metadata = HashMap::from([(MAP_STORAGE_LAYOUT_KEY.to_string(), "default".to_string())]);
        assert!(!has_shredding_metadata(Some(&metadata)));
        assert!(!has_shredding_metadata(Some(&HashMap::new())));
    }

    // -- Write plan (MapSharedShreddingWritePlanTest / RowConverterTest) --

    #[test]
    fn test_infer_num_columns() {
        let config = |max_columns: usize| MapShreddingFieldConfig {
            field_index: 0,
            field_name: "tags".to_string(),
            max_columns,
        };
        let fields = vec![map_field(0, "tags", bigint_type())];
        let batch = logical_batch(
            &fields,
            vec![Arc::new(build_map_array(&[Some(vec![
                ("a", Some(1)),
                ("b", Some(2)),
                ("c", Some(3)),
            ])]))],
        );
        // Inferred from the first row, capped at max-columns.
        assert_eq!(
            infer_num_columns(&config(4), std::slice::from_ref(&batch)).unwrap(),
            3
        );
        assert_eq!(
            infer_num_columns(&config(2), std::slice::from_ref(&batch)).unwrap(),
            2
        );
        // No rows sampled -> max-columns.
        assert_eq!(infer_num_columns(&config(4), &[]).unwrap(), 4);
        let empty_batch = logical_batch(&fields, vec![Arc::new(build_map_array(&[]))]);
        assert_eq!(infer_num_columns(&config(4), &[empty_batch]).unwrap(), 4);
        // A null first row still counts as sampled (width 0 -> clamped to 1).
        let null_batch = logical_batch(&fields, vec![Arc::new(build_map_array(&[None]))]);
        assert_eq!(infer_num_columns(&config(4), &[null_batch]).unwrap(), 1);
    }

    #[test]
    fn test_write_plan_convert_and_metadata() {
        let logical_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            map_field(1, "tags", bigint_type()),
        ];
        let configs = vec![MapShreddingFieldConfig {
            field_index: 1,
            field_name: "tags".to_string(),
            max_columns: 4,
        }];
        // No sampled rows -> num_columns = max_columns.
        let mut plan = MapShreddingWritePlan::infer(&logical_fields, &configs, &[]).unwrap();
        assert_eq!(plan.physical_fields().len(), 2);

        let batch = logical_batch(
            &logical_fields,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(build_map_array(&[Some(vec![
                    ("a", Some(10)),
                    ("b", Some(20)),
                    ("c", Some(30)),
                ])])),
            ],
        );
        let physical = plan.to_physical_batch(&batch).unwrap();
        assert_eq!(
            physical
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        let tags = physical
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        // __field_mapping = [0, 1, 2, -1]
        let mapping = tags.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let mapping_values = mapping
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            mapping_values.iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2), Some(-1)]
        );
        // __col_0..2 = 10, 20, 30; __col_3 and __overflow are null.
        assert_eq!(
            tags.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            tags.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(
            tags.column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            30
        );
        assert!(tags.column(4).is_null(0));
        assert!(tags.column(5).is_null(0));

        let metadata = plan.field_metadata(Some("none")).unwrap();
        assert_eq!(metadata.len(), 1);
        let tags_metadata = metadata.get("tags").unwrap();
        assert_eq!(
            tags_metadata.get(FIELD_DICT_COMPRESSION_KEY).unwrap(),
            "none"
        );
        let field_meta = deserialize_metadata(tags_metadata).unwrap();
        assert_eq!(
            field_meta.name_to_id,
            BTreeMap::from([
                ("a".to_string(), 0),
                ("b".to_string(), 1),
                ("c".to_string(), 2)
            ])
        );
        assert_eq!(field_meta.num_columns, 4);
        assert_eq!(field_meta.max_row_width, 3);
    }

    #[test]
    fn test_write_plan_rejects_null_map_key() {
        let logical_fields = vec![map_field(0, "tags", bigint_type())];
        let configs = vec![MapShreddingFieldConfig {
            field_index: 0,
            field_name: "tags".to_string(),
            max_columns: 2,
        }];
        let mut plan = MapShreddingWritePlan::infer(&logical_fields, &configs, &[]).unwrap();

        let map_type = DataType::Map(MapType::new(string_type(), bigint_type()));
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(&map_type).unwrap()
        else {
            panic!("map type must convert to Arrow Map")
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type().clone() else {
            panic!("map entries must be Struct")
        };
        // Null keys violate the non-nullable "key" field, so build the
        // entries unchecked: readers of external data can still produce such
        // arrays, and the writer must reject them.
        let entries = unsafe {
            StructArray::new_unchecked(
                entry_fields,
                vec![
                    Arc::new(StringArray::from(vec![None, Some("b")])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                ],
                None,
            )
        };
        let map_array = MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            entries,
            None,
            ordered,
        );
        let batch = logical_batch(&logical_fields, vec![Arc::new(map_array)]);
        let err = plan.to_physical_batch(&batch).unwrap_err();
        assert!(
            err.to_string()
                .contains("Shared-shredding MAP keys cannot be null for field: tags"),
            "unexpected error: {err}"
        );
    }

    // -- Read plan (MapSharedShreddingReadPlanTest) --

    #[test]
    fn test_read_without_overflow_column() {
        let logical_fields = vec![map_field(0, "metrics", bigint_type())];
        let meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
            field_to_columns: BTreeMap::new(),
            overflow_set: BTreeSet::new(),
            num_columns: 3,
            max_row_width: 2,
        };
        let physical_type = build_physical_struct_type(&bigint_type(), 3, false).unwrap();
        let struct_fields = physical_struct_fields(&bigint_type(), 3, false);
        let ArrowDataType::List(mapping_element) = struct_fields[0].data_type().clone() else {
            panic!("__field_mapping must be List")
        };
        let struct_array = StructArray::try_new(
            struct_fields,
            vec![
                Arc::new(mapping_list(&mapping_element, vec![0, -1, 1])),
                Arc::new(Int64Array::from(vec![Some(10)])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![Some(20)])),
            ],
            None,
        )
        .unwrap();
        let assembled = assemble_single(
            "metrics",
            &physical_type,
            &logical_fields,
            &meta,
            struct_array,
        );
        let rows = map_array_to_rows(assembled.as_any().downcast_ref::<MapArray>().unwrap());
        assert_eq!(
            rows,
            vec![Some(vec![
                ("a".to_string(), Some(10)),
                ("b".to_string(), Some(20))
            ])]
        );
    }

    #[test]
    fn test_read_overflow_only_when_overflow_column_exists() {
        let logical_fields = vec![map_field(0, "metrics", bigint_type())];
        let meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([("a".to_string(), 0), ("overflowed".to_string(), 1)]),
            field_to_columns: BTreeMap::new(),
            overflow_set: BTreeSet::from([1]),
            num_columns: 1,
            max_row_width: 1,
        };
        let physical_type = build_physical_struct_type(&bigint_type(), 1, true).unwrap();
        let struct_fields = physical_struct_fields(&bigint_type(), 1, true);
        let ArrowDataType::List(mapping_element) = struct_fields[0].data_type().clone() else {
            panic!("__field_mapping must be List")
        };
        let ArrowDataType::Map(overflow_entries_field, _) = struct_fields[2].data_type().clone()
        else {
            panic!("__overflow must be Map")
        };
        let ArrowDataType::Struct(overflow_entry_fields) =
            overflow_entries_field.data_type().clone()
        else {
            panic!("__overflow entries must be Struct")
        };
        let overflow_entries = StructArray::try_new(
            overflow_entry_fields,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(30)])),
            ],
            None,
        )
        .unwrap();
        let overflow = MapArray::new(
            overflow_entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            overflow_entries,
            None,
            false,
        );
        let struct_array = StructArray::try_new(
            struct_fields,
            vec![
                Arc::new(mapping_list(&mapping_element, vec![-1])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(overflow),
            ],
            None,
        )
        .unwrap();
        let assembled = assemble_single(
            "metrics",
            &physical_type,
            &logical_fields,
            &meta,
            struct_array,
        );
        let rows = map_array_to_rows(assembled.as_any().downcast_ref::<MapArray>().unwrap());
        assert_eq!(rows, vec![Some(vec![("overflowed".to_string(), Some(30))])]);
    }

    #[test]
    fn test_read_preserves_null_values() {
        let logical_fields = vec![map_field(0, "metrics", bigint_type())];
        let meta = MapSharedShreddingFieldMeta {
            name_to_id: BTreeMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
            field_to_columns: BTreeMap::new(),
            overflow_set: BTreeSet::new(),
            num_columns: 3,
            max_row_width: 2,
        };
        let physical_type = build_physical_struct_type(&bigint_type(), 3, false).unwrap();
        let struct_fields = physical_struct_fields(&bigint_type(), 3, false);
        let ArrowDataType::List(mapping_element) = struct_fields[0].data_type().clone() else {
            panic!("__field_mapping must be List")
        };
        let struct_array = StructArray::try_new(
            struct_fields,
            vec![
                Arc::new(mapping_list(&mapping_element, vec![0, 1, -1])),
                Arc::new(Int64Array::from(vec![Some(10)])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![Some(20)])),
            ],
            None,
        )
        .unwrap();
        let assembled = assemble_single(
            "metrics",
            &physical_type,
            &logical_fields,
            &meta,
            struct_array,
        );
        let rows = map_array_to_rows(assembled.as_any().downcast_ref::<MapArray>().unwrap());
        assert_eq!(
            rows,
            vec![Some(vec![
                ("a".to_string(), Some(10)),
                ("b".to_string(), None)
            ])]
        );
    }

    // -- End-to-end write -> metadata -> read roundtrip --

    #[test]
    fn test_sparse_rows_with_non_nullable_values_roundtrip() {
        let value_type = bigint_type().copy_with_nullable(false).unwrap();
        let logical_fields = vec![map_field(0, "tags", value_type)];
        let configs = vec![MapShreddingFieldConfig {
            field_index: 0,
            field_name: "tags".to_string(),
            max_columns: 2,
        }];
        let rows = vec![vec![("a", 10), ("b", 20)], vec![("a", 30)]];
        let batch = logical_batch(
            &logical_fields,
            vec![Arc::new(build_non_nullable_map_array(&rows))],
        );

        let mut plan = MapShreddingWritePlan::infer(&logical_fields, &configs, &[]).unwrap();
        let physical = plan.to_physical_batch(&batch).unwrap();
        let field_metadata = plan.field_metadata(Some("zstd")).unwrap();
        let file_fields: Vec<ArrowField> = physical
            .schema()
            .fields()
            .iter()
            .map(|field| match field_metadata.get(field.name()) {
                Some(extra) => field.as_ref().clone().with_metadata(extra.clone()),
                None => field.as_ref().clone(),
            })
            .collect();
        let read_plan =
            MapShreddingReadPlan::create(&logical_fields, &ArrowSchema::new(file_fields))
                .unwrap()
                .expect("read plan expected");
        let assembled = read_plan.assemble_batch(&physical).unwrap();

        assert_eq!(
            map_array_to_rows(
                assembled
                    .column(0)
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .unwrap()
            ),
            vec![
                Some(vec![
                    ("a".to_string(), Some(10)),
                    ("b".to_string(), Some(20)),
                ]),
                Some(vec![("a".to_string(), Some(30))]),
            ]
        );
    }

    #[test]
    fn test_write_read_roundtrip() {
        let logical_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            map_field(1, "tags", bigint_type()),
        ];
        let configs = vec![MapShreddingFieldConfig {
            field_index: 1,
            field_name: "tags".to_string(),
            max_columns: 2,
        }];
        let rows: Vec<Option<Vec<(&str, Option<i64>)>>> = vec![
            Some(vec![("a", Some(10)), ("b", None), ("c", Some(30))]), // c overflows
            None,                                                      // null map
            Some(vec![]),                                              // empty map
            Some(vec![("b", Some(40)), ("a", Some(50))]),
        ];
        let mut plan = MapShreddingWritePlan::infer(&logical_fields, &configs, &[]).unwrap();
        let batch = logical_batch(
            &logical_fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(build_map_array(&rows)),
            ],
        );
        let physical = plan.to_physical_batch(&batch).unwrap();

        // Physical layout of the first row: mapping [0, 1], col0 = 10,
        // col1 = null, overflow = {2: 30}.
        let tags = physical
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let mapping = tags.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let mapping_values = mapping
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            mapping_values.iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(-1), Some(-1), Some(1), Some(0)]
        );
        let col0 = tags
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            col0.iter().collect::<Vec<_>>(),
            vec![Some(10), None, None, Some(40)]
        );
        let overflow = tags.column(3).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!overflow.is_null(0));
        assert!(overflow.is_null(1));
        assert!(overflow.is_null(2));
        assert!(overflow.is_null(3));
        assert_eq!(
            overflow
                .keys()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );

        // Metadata accumulated across the file.
        let field_metadata = plan.field_metadata(Some("zstd")).unwrap();
        let field_meta = deserialize_metadata(field_metadata.get("tags").unwrap()).unwrap();
        assert_eq!(
            field_meta,
            MapSharedShreddingFieldMeta {
                name_to_id: BTreeMap::from([
                    ("a".to_string(), 0),
                    ("b".to_string(), 1),
                    ("c".to_string(), 2),
                ]),
                field_to_columns: BTreeMap::from([(0, vec![0, 1]), (1, vec![0, 1])]),
                overflow_set: BTreeSet::from([2]),
                num_columns: 2,
                max_row_width: 3,
            }
        );

        // Commit the metadata into the physical schema like the parquet
        // writer does, then read back through the read plan.
        let physical_schema = physical.schema();
        let file_fields: Vec<ArrowField> = physical_schema
            .fields()
            .iter()
            .map(|field| match field_metadata.get(field.name()) {
                Some(extra) => field.as_ref().clone().with_metadata(extra.clone()),
                None => field.as_ref().clone(),
            })
            .collect();
        let file_schema = ArrowSchema::new(file_fields);
        let read_plan = MapShreddingReadPlan::create(&logical_fields, &file_schema)
            .unwrap()
            .expect("read plan expected");
        let assembled = read_plan.assemble_batch(&physical).unwrap();

        assert_eq!(
            assembled
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3), Some(4)]
        );
        let assembled_rows = map_array_to_rows(
            assembled
                .column(1)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap(),
        );
        let expected_rows: Vec<Option<Vec<(String, Option<i64>)>>> = rows
            .iter()
            .map(|row| {
                row.as_ref().map(|entries| {
                    entries
                        .iter()
                        .map(|(key, value)| (key.to_string(), *value))
                        .collect()
                })
            })
            .collect();
        assert_eq!(assembled_rows, expected_rows);
    }
}
