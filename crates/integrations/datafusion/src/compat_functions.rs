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
use std::sync::Arc;

use base64::Engine;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::compute::{cast, kernels::numeric};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::types::{logical_int64, logical_string, NativeType};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

pub(crate) fn register_compat_functions(ctx: &SessionContext) {
    for (name, alias) in [
        ("current_date", "curdate"),
        ("to_date", "date"),
        ("to_timestamp_millis", "timestamp_millis"),
        ("to_timestamp", "to_timestamp_ltz"),
    ] {
        ctx.register_udf(ScalarUDF::from(ConfiguredAlias {
            name: alias,
            inner: Arc::clone(&ctx.state().scalar_functions()[name]),
        }));
    }
    for kind in [
        Kind::Mod,
        Kind::DateSub,
        Kind::RegexpExtract,
        Kind::RegexpExtractAll,
        Kind::Sha2,
        Kind::ToBase64,
    ] {
        let signature = match kind {
            Kind::Mod => Signature::user_defined(Volatility::Immutable),
            Kind::DateSub => Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Date32, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            Kind::RegexpExtract | Kind::RegexpExtractAll => Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            Kind::Sha2 => Signature::one_of(
                [
                    TypeSignatureClass::Native(logical_string()),
                    TypeSignatureClass::Binary,
                ]
                .into_iter()
                .map(|input| {
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(input),
                        Coercion::new_implicit(
                            TypeSignatureClass::Native(logical_int64()),
                            vec![TypeSignatureClass::Integer],
                            NativeType::Int64,
                        ),
                    ])
                })
                .collect(),
                Volatility::Immutable,
            ),
            Kind::ToBase64 => Signature::one_of(
                [
                    TypeSignatureClass::Native(logical_string()),
                    TypeSignatureClass::Binary,
                ]
                .into_iter()
                .map(|input| TypeSignature::Coercible(vec![Coercion::new_exact(input)]))
                .collect(),
                Volatility::Immutable,
            ),
        };
        ctx.register_udf(ScalarUDF::from(CompatFunction { kind, signature }));
    }
}

// DataFusion 54's with_aliases wrapper does not forward configuration updates.
// Keep the native function registered separately and forward timezone changes
// and simplification to the alias's own native implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConfiguredAlias {
    name: &'static str,
    inner: Arc<ScalarUDF>,
}

impl ScalarUDFImpl for ConfiguredAlias {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.inner().return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner.inner().with_updated_config(config).map(|inner| {
            ScalarUDF::from(Self {
                name: self.name,
                inner: Arc::new(inner),
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Mod,
    DateSub,
    RegexpExtract,
    RegexpExtractAll,
    Sha2,
    ToBase64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompatFunction {
    kind: Kind,
    signature: Signature,
}

impl ScalarUDFImpl for CompatFunction {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Mod => "mod",
            Kind::DateSub => "date_sub",
            Kind::RegexpExtract => "regexp_extract",
            Kind::RegexpExtractAll => "regexp_extract_all",
            Kind::Sha2 => "sha2",
            Kind::ToBase64 => "to_base64",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = types else {
            return Err(DataFusionError::Plan("mod expects two arguments".into()));
        };
        let (left, right) =
            BinaryTypeCoercer::new(left, &Operator::Modulo, right).get_input_types()?;
        Ok(vec![left, right])
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if self.kind == Kind::Mod {
            return BinaryTypeCoercer::new(&types[0], &Operator::Modulo, &types[1])
                .get_result_type();
        }
        Ok(match self.kind {
            Kind::Mod => unreachable!("handled above"),
            Kind::DateSub => DataType::Date32,
            Kind::RegexpExtractAll => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            _ => DataType::Utf8,
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let output: ArrayRef = match self.kind {
            Kind::Mod => numeric::rem(&arrays[0].as_ref(), &arrays[1].as_ref())?,
            Kind::DateSub => {
                let dates = cast(&arrays[0], &DataType::Date32)?;
                let dates = dates
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| error("date_sub expects dates"))?;
                let days = arrays[1]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| error("date_sub expects integer days"))?;
                let values = dates
                    .iter()
                    .zip(days.iter())
                    .map(|(date, days)| match (date, days) {
                        (Some(date), Some(days)) => i64::from(date)
                            .checked_sub(days)
                            .and_then(|value| i32::try_from(value).ok())
                            .map(Some)
                            .ok_or_else(|| error("date_sub date overflow")),
                        _ => Ok(None),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(Date32Array::from(values))
            }
            Kind::RegexpExtract | Kind::RegexpExtractAll => {
                extract_regex(&arrays, self.kind == Kind::RegexpExtractAll)?
            }
            Kind::Sha2 | Kind::ToBase64 => {
                // Preserve the byte family during planning, then normalize its
                // Arrow representation without decoding binary data as UTF-8.
                let input_type = if NativeType::from(arrays[0].data_type()).is_binary() {
                    DataType::Binary
                } else {
                    DataType::Utf8
                };
                let input = cast(&arrays[0], &input_type)?;
                let mut result = StringBuilder::new();
                for row in 0..arrays[0].len() {
                    if input.is_null(row)
                        || arrays
                            .iter()
                            .skip(1)
                            .any(|array| array.data_type() == &DataType::Null || array.is_null(row))
                    {
                        result.append_null();
                        continue;
                    }
                    let bytes = if let Some(array) = input.as_any().downcast_ref::<BinaryArray>() {
                        array.value(row)
                    } else {
                        input
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| error("expected string or binary"))?
                            .value(row)
                            .as_bytes()
                    };
                    if self.kind == Kind::ToBase64 {
                        result
                            .append_value(base64::engine::general_purpose::STANDARD.encode(bytes));
                    } else {
                        let bits = arrays[1]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| error("sha2 expects integer bit length"))?
                            .value(row);
                        let digest = match bits {
                            0 | 256 => Some(format!("{:x}", Sha256::digest(bytes))),
                            224 => Some(format!("{:x}", Sha224::digest(bytes))),
                            384 => Some(format!("{:x}", Sha384::digest(bytes))),
                            512 => Some(format!("{:x}", Sha512::digest(bytes))),
                            _ => None,
                        };
                        result.append_option(digest);
                    }
                }
                Arc::new(result.finish())
            }
        };
        if args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn extract_regex(arrays: &[ArrayRef], all: bool) -> Result<ArrayRef> {
    let text = cast(&arrays[0], &DataType::Utf8)?;
    let pattern = cast(&arrays[1], &DataType::Utf8)?;
    let text = text.as_any().downcast_ref::<StringArray>().unwrap();
    let pattern = pattern.as_any().downcast_ref::<StringArray>().unwrap();
    let index = arrays
        .get(2)
        .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap());
    let mut cache = HashMap::<&str, Regex>::new();
    let mut strings = StringBuilder::new();
    let mut lists = ListBuilder::new(StringBuilder::new());
    for row in 0..text.len() {
        if arrays.iter().any(|array| array.is_null(row)) {
            strings.append_null();
            lists.append(false);
            continue;
        }
        let group = usize::try_from(index.map(|index| index.value(row)).unwrap_or(1))
            .map_err(|_| error("regexp_extract group index must be non-negative"))?;
        let regex = match cache.entry(pattern.value(row)) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let regex = Regex::new(entry.key())
                    .map_err(|e| error(format!("Invalid regular expression: {e}")))?;
                entry.insert(regex)
            }
        };
        if group >= regex.captures_len() {
            return Err(error(format!(
                "regexp_extract group index {group} exceeds {}",
                regex.captures_len() - 1
            )));
        }
        if all {
            for capture in regex.captures_iter(text.value(row)) {
                lists
                    .values()
                    .append_value(capture.get(group).map(|m| m.as_str()).unwrap_or(""));
            }
            lists.append(true);
        } else {
            let capture = regex.captures(text.value(row));
            strings.append_value(
                capture
                    .as_ref()
                    .and_then(|capture| capture.get(group))
                    .map(|m| m.as_str())
                    .unwrap_or(""),
            );
        }
    }
    Ok(if all {
        Arc::new(lists.finish())
    } else {
        Arc::new(strings.finish())
    })
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
