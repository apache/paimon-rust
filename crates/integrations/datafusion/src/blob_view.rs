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

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BinaryBuilder, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    StringArray, StringViewArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use paimon::catalog::Catalog;
use paimon::spec::BlobViewStruct;

use crate::error::to_datafusion_error;
use crate::runtime::block_on_with_runtime;
use crate::table_function_args::parse_table_identifier;

const FUNCTION_NAME: &str = "blob_view";

pub fn register_blob_view(ctx: &SessionContext, catalog: Arc<dyn Catalog>, default_database: &str) {
    ctx.register_udf(ScalarUDF::from(BlobViewFunc::new(
        catalog,
        default_database,
    )));
}

#[derive(Clone)]
struct BlobViewFunc {
    catalog: Arc<dyn Catalog>,
    default_database: String,
    signature: Signature,
    aliases: Vec<String>,
}

impl BlobViewFunc {
    fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
            signature: Signature::any(3, Volatility::Immutable),
            aliases: vec!["sys.blob_view".to_string()],
        }
    }

    fn field_id(&self, table_name: &str, field_name: &str) -> DFResult<i32> {
        let identifier = parse_table_identifier(FUNCTION_NAME, table_name, &self.default_database)?;
        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { catalog.get_table(&identifier).await },
            "blob_view: catalog access thread panicked",
        )
        .map_err(to_datafusion_error)?;

        let field = table
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == field_name)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "blob_view: cannot find blob field {field_name} in upstream table {table_name}"
                ))
            })?;
        if !field.data_type().is_blob_type() {
            return Err(DataFusionError::Plan(format!(
                "blob_view: field {field_name} in upstream table {table_name} is not a BLOB field"
            )));
        }
        Ok(field.id())
    }
}

impl std::fmt::Debug for BlobViewFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobViewFunc")
            .field("default_database", &self.default_database)
            .field("aliases", &self.aliases)
            .finish()
    }
}

impl PartialEq for BlobViewFunc {
    fn eq(&self, other: &Self) -> bool {
        self.default_database == other.default_database
            && Arc::ptr_eq(&self.catalog, &other.catalog)
    }
}

impl Eq for BlobViewFunc {}

impl Hash for BlobViewFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        FUNCTION_NAME.hash(state);
        self.default_database.hash(state);
        let ptr = Arc::as_ptr(&self.catalog) as *const () as usize;
        ptr.hash(state);
    }
}

impl ScalarUDFImpl for BlobViewFunc {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(ArrowDataType::Binary)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            FUNCTION_NAME,
            ArrowDataType::Binary,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Plan(
                "blob_view expects 3 arguments: (table_name, field_name_or_id, row_id)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let table_names = arrays[0].as_ref();
        let fields = arrays[1].as_ref();
        let row_ids = arrays[2].as_ref();
        let mut field_id_cache = HashMap::new();
        let mut builder = BinaryBuilder::new();

        for row in 0..table_names.len() {
            let Some(table_name) = string_at(table_names, row)? else {
                builder.append_null();
                continue;
            };
            let Some(row_id) = int64_at(row_ids, row, "row_id")? else {
                builder.append_null();
                continue;
            };

            let field_id = if is_string_array(fields) {
                let Some(field_name) = string_at(fields, row)? else {
                    builder.append_null();
                    continue;
                };
                let cache_key = (table_name.clone(), field_name.clone());
                match field_id_cache.get(&cache_key) {
                    Some(field_id) => *field_id,
                    None => {
                        let field_id = self.field_id(&table_name, &field_name)?;
                        field_id_cache.insert(cache_key, field_id);
                        field_id
                    }
                }
            } else {
                match int64_at(fields, row, "field_id")? {
                    Some(field_id) => i32::try_from(field_id).map_err(|_| {
                        DataFusionError::Plan(format!(
                            "blob_view: field_id {field_id} is outside i32 range"
                        ))
                    })?,
                    None => {
                        builder.append_null();
                        continue;
                    }
                }
            };

            let identifier =
                parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;
            let value = BlobViewStruct::new(identifier, field_id, row_id)
                .serialize()
                .map_err(to_datafusion_error)?;
            builder.append_value(value);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn is_string_array(array: &dyn Array) -> bool {
    matches!(
        array.data_type(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View
    )
}

fn string_at(array: &dyn Array, row: usize) -> DFResult<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(values.value(row).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(values.value(row).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(values.value(row).to_string()));
    }
    Err(DataFusionError::Plan(format!(
        "blob_view: expected string argument, got {:?}",
        array.data_type()
    )))
}

fn int64_at(array: &dyn Array, row: usize, name: &str) -> DFResult<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! downcast_int {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                return Ok(Some(values.value(row) as i64));
            }
        };
    }
    downcast_int!(Int8Array);
    downcast_int!(Int16Array);
    downcast_int!(Int32Array);
    downcast_int!(Int64Array);
    downcast_int!(UInt8Array);
    downcast_int!(UInt16Array);
    downcast_int!(UInt32Array);
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return i64::try_from(values.value(row)).map(Some).map_err(|_| {
            DataFusionError::Plan(format!(
                "blob_view: {name} {} is outside i64 range",
                values.value(row)
            ))
        });
    }
    Err(DataFusionError::Plan(format!(
        "blob_view: expected integer {name}, got {:?}",
        array.data_type()
    )))
}
