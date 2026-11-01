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

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, LargeStringArray, StringArray, StringBuilder, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use paimon::variant::{GenericVariant, VariantKind, VariantRef};

use crate::variant_functions::{variant_arrow_type, variant_children};

pub(crate) fn register_json_functions(ctx: &SessionContext) {
    for (name, kind, aliases) in [
        ("json_value", Kind::Scalar, vec!["json_extract_scalar"]),
        (
            "get_json_object",
            Kind::Fragment,
            vec!["get_json_string", "json_extract_string", "json_get_str"],
        ),
        ("json_valid", Kind::Valid, vec![]),
        ("json_format", Kind::Format, vec![]),
    ] {
        let signature = match kind {
            Kind::Scalar | Kind::Fragment => Signature::string(2, Volatility::Immutable),
            Kind::Valid => Signature::string(1, Volatility::Immutable),
            Kind::Format => Signature::any(1, Volatility::Immutable),
        };
        ctx.register_udf(
            ScalarUDF::from(JsonFunction {
                name,
                kind,
                signature,
            })
            .with_aliases(aliases),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Kind {
    Scalar,
    Fragment,
    Valid,
    Format,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JsonFunction {
    name: &'static str,
    kind: Kind,
    signature: Signature,
}

impl ScalarUDFImpl for JsonFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.kind == Kind::Format
            && !matches!(
                arg_types[0],
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
            && arg_types[0] != variant_arrow_type()
        {
            return Err(DataFusionError::Plan(
                "json_format expects JSON text or a VARIANT".into(),
            ));
        }
        Ok(if self.kind == Kind::Valid {
            DataType::Boolean
        } else {
            DataType::Utf8
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_ref();
        let mut strings = StringBuilder::new();
        let mut booleans = BooleanBuilder::new();
        let variant = if self.kind == Kind::Format && input.data_type() == &variant_arrow_type() {
            variant_children(input)?
        } else {
            None
        };
        for row in 0..input.len() {
            if input.data_type() == &DataType::Null || input.is_null(row) {
                strings.append_null();
                booleans.append_null();
                continue;
            }
            if let Some((values, metadata)) = variant {
                let value = VariantRef::new(values.value(row), metadata.value(row), 0)
                    .and_then(|value| value.to_json())
                    .map_err(crate::to_datafusion_error)?;
                strings.append_value(value);
                continue;
            }
            let text = string_at(input, row)?;
            let parsed = GenericVariant::parse_json(text);
            if self.kind == Kind::Valid {
                booleans.append_value(parsed.is_ok());
                continue;
            }
            if self.kind == Kind::Format {
                let parsed = parsed.map_err(|error| {
                    DataFusionError::Execution(format!("Invalid JSON: {error}"))
                })?;
                strings.append_value(parsed.to_json().map_err(crate::to_datafusion_error)?);
                continue;
            }
            let path = arrays[1].as_ref();
            let extracted = if path.is_null(row) {
                None
            } else {
                parsed.ok().and_then(|json| {
                    extract(&json, string_at(path, row).ok()?, self.kind == Kind::Scalar)
                })
            };
            strings.append_option(extracted);
        }
        let output: ArrayRef = if self.kind == Kind::Valid {
            Arc::new(booleans.finish())
        } else {
            Arc::new(strings.finish())
        };
        if args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn extract(json: &GenericVariant, path: &str, scalar_only: bool) -> Option<String> {
    let value = json.get_path(path).ok()??;
    match value.kind().ok()? {
        VariantKind::Null => None,
        VariantKind::Object | VariantKind::Array if scalar_only => None,
        VariantKind::String => value.get_string().ok(),
        _ => value.to_json().ok(),
    }
}

fn string_at(array: &dyn Array, row: usize) -> Result<&str> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(array.value(row));
    }
    Err(DataFusionError::Execution(format!(
        "JSON function expected a string, got {}",
        array.data_type()
    )))
}
