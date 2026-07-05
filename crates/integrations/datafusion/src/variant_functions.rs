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
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, LargeStringArray, StringArray,
    StringViewArray, StructArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use paimon::variant::{GenericVariant, VariantDecimal, VariantKind, VariantRef};

pub fn register_variant_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new(false)));
    ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new(true)));
    ctx.register_udf(ScalarUDF::from(IsVariantNullFunc::new()));
    ctx.register_udf(ScalarUDF::from(VariantGetFunc::new(false)));
    ctx.register_udf(ScalarUDF::from(VariantGetFunc::new(true)));
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParseJsonFunc {
    try_parse: bool,
    signature: Signature,
}

impl ParseJsonFunc {
    fn new(try_parse: bool) -> Self {
        Self {
            try_parse,
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ParseJsonFunc {
    fn name(&self) -> &str {
        if self.try_parse {
            "try_parse_json"
        } else {
            "parse_json"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(variant_arrow_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            variant_arrow_type(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err(format!("{} expects 1 argument", self.name()));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_ref();
        let mut values = Vec::with_capacity(input.len());
        for row in 0..input.len() {
            let Some(json) = string_at(input, row)? else {
                values.push(None);
                continue;
            };
            match GenericVariant::parse_json(&json) {
                Ok(variant) => values.push(Some(variant)),
                Err(e) if self.try_parse => {
                    let _ = e;
                    values.push(None);
                }
                Err(e) => return Err(to_df_error(e)),
            }
        }
        Ok(ColumnarValue::Array(variant_array(values)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IsVariantNullFunc {
    signature: Signature,
}

impl IsVariantNullFunc {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsVariantNullFunc {
    fn name(&self) -> &str {
        "is_variant_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(ArrowDataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            ArrowDataType::Boolean,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err("is_variant_null expects 1 argument");
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_ref();
        let mut builder = BooleanBuilder::new();
        let Some((values, metadata)) = variant_children(input)? else {
            for _ in 0..input.len() {
                builder.append_value(false);
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        };

        for row in 0..input.len() {
            if input.is_null(row) {
                builder.append_value(false);
            } else {
                let variant = VariantRef::new(values.value(row), metadata.value(row), 0)
                    .map_err(to_df_error)?;
                builder.append_value(variant.is_null().map_err(to_df_error)?);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VariantGetFunc {
    try_get: bool,
    signature: Signature,
}

impl VariantGetFunc {
    fn new(try_get: bool) -> Self {
        Self {
            try_get,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantGetFunc {
    fn name(&self) -> &str {
        if self.try_get {
            "try_variant_get"
        } else {
            "variant_get"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        internal_err("return_field_from_args should be used for variant_get")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        if args.arg_fields.len() != 2 && args.arg_fields.len() != 3 {
            return plan_err(format!("{} expects 2 or 3 arguments", self.name()));
        }
        let output = match args.arg_fields.len() {
            2 => variant_get_output_type(None)?,
            3 => {
                let Some(type_arg) = args.scalar_arguments.get(2).and_then(|v| *v) else {
                    return plan_err("variant_get type argument must be a string literal");
                };
                variant_get_output_type(Some(type_arg))?
            }
            _ => unreachable!("argument count checked above"),
        };
        Ok(Arc::new(Field::new(
            self.name(),
            output.arrow_type().clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 3 {
            return plan_err(format!("{} expects 2 or 3 arguments", self.name()));
        }
        let output = if args.return_type() == &variant_arrow_type() {
            VariantGetOutput::Variant
        } else {
            VariantGetOutput::Scalar(args.return_type().clone())
        };
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let variants = arrays[0].as_ref();
        let paths = arrays[1].as_ref();
        let Some((values, metadata)) = variant_children(variants)? else {
            return Ok(ColumnarValue::Array(null_array(
                output.arrow_type(),
                variants.len(),
            )));
        };

        match output {
            VariantGetOutput::Variant => {
                let mut result = Vec::with_capacity(variants.len());
                for row in 0..variants.len() {
                    result.push(self.variant_at_path(variants, values, metadata, paths, row)?);
                }
                Ok(ColumnarValue::Array(variant_array(result)?))
            }
            VariantGetOutput::Scalar(data_type) => {
                let mut scalars = Vec::with_capacity(variants.len());
                for row in 0..variants.len() {
                    match self.variant_at_path_ref(variants, values, metadata, paths, row)? {
                        Some(variant) => scalars.push(cast_variant_to_scalar(
                            variant,
                            &data_type,
                            !self.try_get,
                        )?),
                        None => scalars.push(ScalarValue::try_from(&data_type)?),
                    }
                }
                if scalars.is_empty() {
                    return Ok(ColumnarValue::Array(null_array(&data_type, 0)));
                }
                Ok(ColumnarValue::Array(ScalarValue::iter_to_array(scalars)?))
            }
        }
    }
}

impl VariantGetFunc {
    fn variant_at_path(
        &self,
        variants: &dyn Array,
        values: &BinaryArray,
        metadata: &BinaryArray,
        paths: &dyn Array,
        row: usize,
    ) -> DFResult<Option<GenericVariant>> {
        self.variant_at_path_ref(variants, values, metadata, paths, row)?
            .map(|variant| variant.to_owned_variant().map_err(to_df_error))
            .transpose()
    }

    fn variant_at_path_ref<'a>(
        &self,
        variants: &dyn Array,
        values: &'a BinaryArray,
        metadata: &'a BinaryArray,
        paths: &dyn Array,
        row: usize,
    ) -> DFResult<Option<VariantRef<'a>>> {
        if variants.is_null(row) || paths.is_null(row) {
            return Ok(None);
        }
        let path = string_at(paths, row)?;
        let Some(path) = path else {
            return Ok(None);
        };
        let variant =
            VariantRef::new(values.value(row), metadata.value(row), 0).map_err(to_df_error)?;
        match variant.get_path(&path) {
            Ok(value) => Ok(value),
            Err(e) if self.try_get => {
                let _ = e;
                Ok(None)
            }
            Err(e) => Err(to_df_error(e)),
        }
    }
}

#[derive(Clone, Debug)]
enum VariantGetOutput {
    Variant,
    Scalar(ArrowDataType),
}

impl VariantGetOutput {
    fn arrow_type(&self) -> &ArrowDataType {
        match self {
            Self::Variant => {
                static VARIANT_TYPE: std::sync::LazyLock<ArrowDataType> =
                    std::sync::LazyLock::new(variant_arrow_type);
                &VARIANT_TYPE
            }
            Self::Scalar(data_type) => data_type,
        }
    }
}

fn variant_get_output_type(type_arg: Option<&ScalarValue>) -> DFResult<VariantGetOutput> {
    let Some(type_arg) = type_arg else {
        return Ok(VariantGetOutput::Variant);
    };
    let type_name = match type_arg {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => v,
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            return plan_err("variant_get type argument must not be NULL");
        }
        _ => return plan_err("variant_get type argument must be a string literal"),
    };
    parse_variant_get_type(type_name)
}

fn parse_variant_get_type(type_name: &str) -> DFResult<VariantGetOutput> {
    let normalized = type_name.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "variant" => Ok(VariantGetOutput::Variant),
        "boolean" | "bool" => Ok(VariantGetOutput::Scalar(ArrowDataType::Boolean)),
        "byte" | "tinyint" => Ok(VariantGetOutput::Scalar(ArrowDataType::Int8)),
        "short" | "smallint" => Ok(VariantGetOutput::Scalar(ArrowDataType::Int16)),
        "int" | "integer" => Ok(VariantGetOutput::Scalar(ArrowDataType::Int32)),
        "long" | "bigint" => Ok(VariantGetOutput::Scalar(ArrowDataType::Int64)),
        "float" | "real" => Ok(VariantGetOutput::Scalar(ArrowDataType::Float32)),
        "double" => Ok(VariantGetOutput::Scalar(ArrowDataType::Float64)),
        "string" | "varchar" | "text" => Ok(VariantGetOutput::Scalar(ArrowDataType::Utf8)),
        "decimal" => Ok(VariantGetOutput::Scalar(ArrowDataType::Decimal128(10, 0))),
        _ if normalized.starts_with("decimal(") && normalized.ends_with(')') => {
            let inner = &normalized["decimal(".len()..normalized.len() - 1];
            let Some((precision, scale)) = inner.split_once(',') else {
                return plan_err(format!("Invalid decimal type for variant_get: {type_name}"));
            };
            let precision = precision
                .trim()
                .parse::<u8>()
                .map_err(|e| DataFusionError::Plan(format!("Invalid decimal precision: {e}")))?;
            let scale = scale
                .trim()
                .parse::<i8>()
                .map_err(|e| DataFusionError::Plan(format!("Invalid decimal scale: {e}")))?;
            Ok(VariantGetOutput::Scalar(ArrowDataType::Decimal128(
                precision, scale,
            )))
        }
        _ => plan_err(format!("Unsupported variant_get type: {type_name}")),
    }
}

fn cast_variant_to_scalar(
    variant: VariantRef<'_>,
    target: &ArrowDataType,
    fail_on_error: bool,
) -> DFResult<ScalarValue> {
    if variant.is_null().map_err(to_df_error)? {
        return ScalarValue::try_from(target);
    }
    let result = match target {
        ArrowDataType::Boolean => cast_to_boolean(variant),
        ArrowDataType::Int8 => cast_to_i64(variant).and_then(|v| {
            i8::try_from(v)
                .map(ScalarValue::from)
                .map_err(|_| invalid_cast())
        }),
        ArrowDataType::Int16 => cast_to_i64(variant).and_then(|v| {
            i16::try_from(v)
                .map(ScalarValue::from)
                .map_err(|_| invalid_cast())
        }),
        ArrowDataType::Int32 => cast_to_i64(variant).and_then(|v| {
            i32::try_from(v)
                .map(ScalarValue::from)
                .map_err(|_| invalid_cast())
        }),
        ArrowDataType::Int64 => cast_to_i64(variant).map(ScalarValue::from),
        ArrowDataType::Float32 => {
            cast_to_f64(variant).map(|v| ScalarValue::Float32(Some(v as f32)))
        }
        ArrowDataType::Float64 => cast_to_f64(variant).map(ScalarValue::from),
        ArrowDataType::Utf8 => cast_to_string(variant).map(ScalarValue::from),
        ArrowDataType::Decimal128(precision, scale) => cast_to_decimal(variant, *precision, *scale),
        _ => Err(invalid_cast()),
    };

    match result {
        Ok(value) => Ok(value),
        Err(e) if !fail_on_error => {
            let _ = e;
            ScalarValue::try_from(target)
        }
        Err(e) => Err(e),
    }
}

fn cast_to_boolean(variant: VariantRef<'_>) -> DFResult<ScalarValue> {
    match variant.kind().map_err(to_df_error)? {
        VariantKind::Boolean => Ok(ScalarValue::Boolean(Some(
            variant.get_boolean().map_err(to_df_error)?,
        ))),
        VariantKind::String => match variant
            .get_string()
            .map_err(to_df_error)?
            .to_ascii_lowercase()
            .as_str()
        {
            "true" => Ok(ScalarValue::Boolean(Some(true))),
            "false" => Ok(ScalarValue::Boolean(Some(false))),
            _ => Err(invalid_cast()),
        },
        _ => Err(invalid_cast()),
    }
}

fn cast_to_i64(variant: VariantRef<'_>) -> DFResult<i64> {
    match variant.kind().map_err(to_df_error)? {
        VariantKind::Long
        | VariantKind::Date
        | VariantKind::Timestamp
        | VariantKind::TimestampNtz => variant.get_long().map_err(to_df_error),
        VariantKind::String => variant
            .get_string()
            .map_err(to_df_error)?
            .parse::<i64>()
            .map_err(|_| invalid_cast()),
        VariantKind::Decimal => {
            let decimal = variant.get_decimal().map_err(to_df_error)?;
            rescale_decimal(decimal.unscaled, decimal.scale, 0)
                .and_then(|v| i64::try_from(v).map_err(|_| invalid_cast()))
        }
        _ => Err(invalid_cast()),
    }
}

fn cast_to_f64(variant: VariantRef<'_>) -> DFResult<f64> {
    match variant.kind().map_err(to_df_error)? {
        VariantKind::Long
        | VariantKind::Date
        | VariantKind::Timestamp
        | VariantKind::TimestampNtz => Ok(variant.get_long().map_err(to_df_error)? as f64),
        VariantKind::Double => variant.get_double().map_err(to_df_error),
        VariantKind::Float => Ok(variant.get_float().map_err(to_df_error)? as f64),
        VariantKind::Decimal => {
            let decimal = variant.get_decimal().map_err(to_df_error)?;
            Ok(decimal.unscaled as f64 / 10f64.powi(decimal.scale as i32))
        }
        VariantKind::String => variant
            .get_string()
            .map_err(to_df_error)?
            .parse::<f64>()
            .map_err(|_| invalid_cast()),
        _ => Err(invalid_cast()),
    }
}

fn cast_to_string(variant: VariantRef<'_>) -> DFResult<String> {
    match variant.kind().map_err(to_df_error)? {
        VariantKind::Object | VariantKind::Array => variant.to_json().map_err(to_df_error),
        VariantKind::Boolean => Ok(variant.get_boolean().map_err(to_df_error)?.to_string()),
        VariantKind::Long
        | VariantKind::Date
        | VariantKind::Timestamp
        | VariantKind::TimestampNtz => Ok(variant.get_long().map_err(to_df_error)?.to_string()),
        VariantKind::String => variant.get_string().map_err(to_df_error),
        VariantKind::Double => Ok(variant.get_double().map_err(to_df_error)?.to_string()),
        VariantKind::Decimal => Ok(variant
            .get_decimal()
            .map_err(to_df_error)?
            .to_plain_string()),
        VariantKind::Float => Ok(variant.get_float().map_err(to_df_error)?.to_string()),
        _ => variant.to_json().map_err(to_df_error),
    }
}

fn cast_to_decimal(variant: VariantRef<'_>, precision: u8, scale: i8) -> DFResult<ScalarValue> {
    let unscaled = match variant.kind().map_err(to_df_error)? {
        VariantKind::Long
        | VariantKind::Date
        | VariantKind::Timestamp
        | VariantKind::TimestampNtz => {
            rescale_decimal(variant.get_long().map_err(to_df_error)? as i128, 0, scale)?
        }
        VariantKind::Decimal => {
            let decimal = variant.get_decimal().map_err(to_df_error)?;
            rescale_decimal(decimal.unscaled, decimal.scale, scale)?
        }
        VariantKind::String => {
            let parsed = parse_decimal_string(&variant.get_string().map_err(to_df_error)?)
                .ok_or_else(invalid_cast)?;
            rescale_decimal(parsed.unscaled, parsed.scale, scale)?
        }
        _ => return Err(invalid_cast()),
    };
    if decimal_precision(unscaled) > precision {
        return Err(invalid_cast());
    }
    Ok(ScalarValue::Decimal128(Some(unscaled), precision, scale))
}

fn rescale_decimal(unscaled: i128, from_scale: i8, to_scale: i8) -> DFResult<i128> {
    match to_scale.cmp(&from_scale) {
        std::cmp::Ordering::Equal => Ok(unscaled),
        std::cmp::Ordering::Greater => {
            let factor = 10_i128
                .checked_pow((to_scale - from_scale) as u32)
                .ok_or_else(invalid_cast)?;
            unscaled.checked_mul(factor).ok_or_else(invalid_cast)
        }
        std::cmp::Ordering::Less => {
            let factor = 10_i128
                .checked_pow((from_scale - to_scale) as u32)
                .ok_or_else(invalid_cast)?;
            if unscaled % factor == 0 {
                Ok(unscaled / factor)
            } else {
                Err(invalid_cast())
            }
        }
    }
}

fn parse_decimal_string(input: &str) -> Option<VariantDecimal> {
    let input = input.trim();
    if input.is_empty() || input.contains(['e', 'E']) {
        return None;
    }
    let negative = input.starts_with('-');
    let unsigned = input.strip_prefix('-').unwrap_or(input);
    if unsigned.is_empty()
        || unsigned.matches('.').count() > 1
        || !unsigned.bytes().all(|ch| ch == b'.' || ch.is_ascii_digit())
    {
        return None;
    }
    let scale = unsigned
        .split_once('.')
        .map(|(_, fraction)| fraction.len())
        .unwrap_or(0);
    let digits: String = unsigned
        .bytes()
        .filter(|ch| *ch != b'.')
        .map(char::from)
        .collect();
    let significant = digits.trim_start_matches('0');
    let precision = if significant.is_empty() {
        1
    } else {
        significant.len()
    };
    if precision > 38 || scale > 38 {
        return None;
    }
    let mut unscaled = digits.parse::<i128>().ok()?;
    if negative {
        unscaled = -unscaled;
    }
    Some(VariantDecimal {
        unscaled,
        precision: precision as u8,
        scale: scale as i8,
    })
}

fn decimal_precision(unscaled: i128) -> u8 {
    let mut value = unscaled.unsigned_abs();
    if value == 0 {
        return 1;
    }
    let mut precision = 0;
    while value > 0 {
        precision += 1;
        value /= 10;
    }
    precision
}

fn string_at(array: &dyn Array, row: usize) -> DFResult<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        ArrowDataType::Utf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected Utf8 array".to_string()))?
                .value(row)
                .to_string(),
        )),
        ArrowDataType::LargeUtf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected LargeUtf8 array".to_string()))?
                .value(row)
                .to_string(),
        )),
        ArrowDataType::Utf8View => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected Utf8View array".to_string()))?
                .value(row)
                .to_string(),
        )),
        other => plan_err(format!("Expected string array, got {other:?}")),
    }
}

fn variant_children(array: &dyn Array) -> DFResult<Option<(&BinaryArray, &BinaryArray)>> {
    let ArrowDataType::Struct(fields) = array.data_type() else {
        return Ok(None);
    };
    if fields.len() != 2
        || fields[0].name() != "value"
        || fields[0].data_type() != &ArrowDataType::Binary
        || fields[1].name() != "metadata"
        || fields[1].data_type() != &ArrowDataType::Binary
    {
        return Ok(None);
    }
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected Variant StructArray".to_string()))?;
    let values = array
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected Variant.value BinaryArray".to_string())
        })?;
    let metadata = array
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected Variant.metadata BinaryArray".to_string())
        })?;
    Ok(Some((values, metadata)))
}

fn variant_array(values: Vec<Option<GenericVariant>>) -> DFResult<ArrayRef> {
    let len = values.len();
    let mut value_builder = BinaryBuilder::new();
    let mut metadata_builder = BinaryBuilder::new();
    let mut validities = Vec::with_capacity(len);
    for value in values {
        match value {
            Some(variant) => {
                value_builder.append_value(variant.value());
                metadata_builder.append_value(variant.metadata());
                validities.push(true);
            }
            None => {
                value_builder.append_value(&[] as &[u8]);
                metadata_builder.append_value(&[] as &[u8]);
                validities.push(false);
            }
        }
    }
    let nulls = if validities.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::new(BooleanBuffer::from(validities)))
    };
    let array = StructArray::try_new(
        variant_fields(),
        vec![
            Arc::new(value_builder.finish()),
            Arc::new(metadata_builder.finish()),
        ],
        nulls,
    )?;
    Ok(Arc::new(array))
}

fn variant_arrow_type() -> ArrowDataType {
    paimon::arrow::variant_arrow_type()
}

fn variant_fields() -> Fields {
    match variant_arrow_type() {
        ArrowDataType::Struct(fields) => fields,
        _ => unreachable!("variant_arrow_type must be a struct"),
    }
}

fn null_array(data_type: &ArrowDataType, len: usize) -> ArrayRef {
    datafusion::arrow::array::new_null_array(data_type, len)
}

fn invalid_cast() -> DataFusionError {
    DataFusionError::Execution("Invalid Variant cast".to_string())
}

fn to_df_error(error: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn plan_err<T>(message: impl Into<String>) -> DFResult<T> {
    Err(DataFusionError::Plan(message.into()))
}

fn internal_err<T>(message: impl Into<String>) -> DFResult<T> {
    Err(DataFusionError::Internal(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};

    async fn collect_one(sql: &str) -> datafusion::arrow::record_batch::RecordBatch {
        let ctx = SessionContext::new();
        register_variant_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        batches.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn parse_json_and_variant_get_scalars() {
        let batch = collect_one(
            r#"
            SELECT
              variant_get(parse_json('{"age":26,"city":"Beijing","nested":{"name":"Alice"},"arr":[1,2,3]}'), '$.age', 'int') AS age,
              variant_get(parse_json('{"age":26,"city":"Beijing","nested":{"name":"Alice"},"arr":[1,2,3]}'), '$.city', 'string') AS city,
              variant_get(parse_json('{"age":26,"city":"Beijing","nested":{"name":"Alice"},"arr":[1,2,3]}'), '$.nested.name', 'string') AS name,
              variant_get(parse_json('{"age":26,"city":"Beijing","nested":{"name":"Alice"},"arr":[1,2,3]}'), '$.arr[1]', 'int') AS arr_value
            "#,
        )
        .await;

        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            26
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Beijing"
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Alice"
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test]
    async fn variant_null_is_distinct_from_sql_null() {
        let batch = collect_one(
            "SELECT is_variant_null(parse_json('null')) AS variant_null, is_variant_null(NULL) AS sql_null",
        )
        .await;
        let variant_null = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let sql_null = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(variant_null.value(0));
        assert!(!sql_null.value(0));
    }

    #[tokio::test]
    async fn try_functions_return_null_on_invalid_input() {
        let batch = collect_one(
            r#"
            SELECT
              try_parse_json('{bad json') AS bad_json,
              try_variant_get(parse_json('{"age":"not an int"}'), '$.age', 'int') AS bad_cast,
              variant_get(parse_json('{}'), '$.missing', 'int') AS missing_path
            "#,
        )
        .await;
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
        assert!(batch.column(2).is_null(0));
    }

    #[tokio::test]
    async fn strict_functions_surface_errors() {
        let ctx = SessionContext::new();
        register_variant_functions(&ctx);
        let err = ctx
            .sql("SELECT parse_json('{bad json')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Expected"));

        let err = ctx
            .sql("SELECT variant_get(parse_json('{\"age\":\"not an int\"}'), '$.age', 'int')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid Variant cast"));
    }

    #[tokio::test]
    async fn variant_get_rejects_non_literal_type_argument() {
        let ctx = SessionContext::new();
        register_variant_functions(&ctx);
        let sql = r#"
            SELECT variant_get(parse_json('{"age":26}'), '$.age', type_name)
            FROM (VALUES ('int')) AS t(type_name)
        "#;
        let err = match ctx.sql(sql).await {
            Ok(df) => df.collect().await.unwrap_err(),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("variant_get type argument must be a string literal"));
    }
}
