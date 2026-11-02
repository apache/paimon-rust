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

use crate::variant_functions::{variant_arrow_type, variant_children};
use base64::Engine;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, ListArray, MapArray, StringArray, StringBuilder,
    StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{
    config::ConfigOptions, tree_node::Transformed, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion::logical_expr::{
    expr::Cast, expr_rewriter::FunctionRewrite, registry::FunctionRegistry, ColumnarValue, Expr,
    ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use paimon::variant::{VariantKind, VariantRef};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Json,
    Object,
    Cast,
    TryCast,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Output {
    kind: Kind,
    signature: Signature,
}
fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
fn udf(kind: Kind) -> ScalarUDF {
    ScalarUDF::from(Output {
        kind,
        signature: Signature::user_defined(Volatility::Immutable),
    })
}
pub(crate) fn register(ctx: &mut SessionContext) {
    SessionContext::register_udf(ctx, udf(Kind::Json));
    SessionContext::register_udf(ctx, udf(Kind::Object));
    let cast = Arc::new(udf(Kind::Cast));
    let try_cast = Arc::new(udf(Kind::TryCast));
    SessionContext::register_udf(ctx, (*cast).clone());
    SessionContext::register_udf(ctx, (*try_cast).clone());
    ctx.register_function_rewrite(Arc::new(VariantStringCast { cast, try_cast }))
        .expect("register Variant CAST rewrite");
}
#[derive(Debug)]
struct VariantStringCast {
    cast: Arc<ScalarUDF>,
    try_cast: Arc<ScalarUDF>,
}
impl FunctionRewrite for VariantStringCast {
    fn name(&self) -> &str {
        "paimon_variant_string_cast"
    }
    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let (input, target, try_cast) = match &expr {
            Expr::Cast(c) => (c.expr.as_ref(), &c.field, false),
            Expr::TryCast(c) => (c.expr.as_ref(), &c.field, true),
            _ => return Ok(Transformed::no(expr)),
        };
        if !matches!(
            target.data_type(),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ) {
            return Ok(Transformed::no(expr));
        }
        let input_type = input.get_type(schema)?;
        let decoded = match &input_type {
            DataType::Dictionary(_, inner) => inner.as_ref(),
            other => other,
        };
        if decoded != &variant_arrow_type() {
            return Ok(Transformed::no(expr));
        }
        let func = if try_cast { &self.try_cast } else { &self.cast };
        let output = func.call(vec![input.clone()]);
        Ok(Transformed::yes(Expr::Cast(Cast::new_from_field(
            Box::new(output),
            Arc::clone(target),
        ))))
    }
}
impl ScalarUDFImpl for Output {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Json => "to_json",
            Kind::Object => "json_object",
            Kind::Cast => "__paimon_variant_string",
            Kind::TryCast => "__paimon_try_variant_string",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if self.kind == Kind::Object {
            if types.len() % 2 != 0 {
                return Err(DataFusionError::Plan(
                    "json_object requires key/value pairs".into(),
                ));
            }
            return Ok(types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    if i % 2 == 0 {
                        DataType::Utf8
                    } else {
                        t.clone()
                    }
                })
                .collect());
        }
        if types.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{} expects one argument",
                self.name()
            )));
        }
        Ok(types.to_vec())
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?
            .iter()
            .map(decoded)
            .collect::<Result<Vec<_>>>()?;
        let len = arrays.first().map_or(args.number_rows.max(1), |a| a.len());
        let mut output = StringBuilder::new();
        for row in 0..len {
            if self.kind == Kind::Object {
                let mut fields = BTreeMap::new();
                for pair in arrays.chunks_exact(2) {
                    if pair[0].is_null(row) {
                        return Err(error("json_object keys cannot be NULL"));
                    }
                    let key = pair[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| error("json_object key must be a string"))?
                        .value(row);
                    fields.insert(key.to_string(), serialize(pair[1].as_ref(), row)?);
                }
                output.append_value(object(fields)?);
            } else if arrays[0].data_type() == &DataType::Null || arrays[0].is_null(row) {
                output.append_null();
            } else if self.kind == Kind::Json {
                output.append_value(serialize(arrays[0].as_ref(), row)?);
            } else {
                let input = &arrays[0];
                let value = (|| {
                    let (values, metadata) = variant_children(input.as_ref())?
                        .ok_or_else(|| error("Expected Variant input"))?;
                    let variant = VariantRef::new(values.value(row), metadata.value(row), 0)
                        .map_err(crate::to_datafusion_error)?;
                    if variant.kind().map_err(crate::to_datafusion_error)? == VariantKind::Null {
                        return Ok(None);
                    }
                    crate::variant_functions::cast_to_string(variant).map(Some)
                })();
                match value {
                    Ok(Some(value)) => output.append_value(value),
                    Ok(None) => output.append_null(),
                    Err(_) if self.kind == Kind::TryCast => output.append_null(),
                    Err(e) => return Err(e),
                }
            }
        }
        let output: ArrayRef = Arc::new(output.finish());
        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}
fn decoded(input: &ArrayRef) -> Result<ArrayRef> {
    match input.data_type() {
        DataType::Dictionary(_, inner) => Ok(cast(input, inner)?),
        _ => Ok(Arc::clone(input)),
    }
}
fn quoted(input: &str) -> Result<String> {
    serde_json::to_string(input).map_err(|e| error(e.to_string()))
}
fn object(fields: BTreeMap<String, String>) -> Result<String> {
    fields
        .into_iter()
        .map(|(key, value)| Ok(format!("{}:{value}", quoted(&key)?)))
        .collect::<Result<Vec<_>>>()
        .map(|fields| format!("{{{}}}", fields.join(",")))
}
fn serialize(input: &dyn Array, row: usize) -> Result<String> {
    if input.data_type() == &DataType::Null || input.is_null(row) {
        return Ok("null".into());
    }
    if input.data_type() == &variant_arrow_type() {
        let (values, metadata) =
            variant_children(input)?.ok_or_else(|| error("Expected Variant input"))?;
        return VariantRef::new(values.value(row), metadata.value(row), 0)
            .and_then(|v| v.to_json())
            .map_err(crate::to_datafusion_error);
    }
    match input.data_type() {
        DataType::Dictionary(_, inner) => serialize(cast(&input.slice(row, 1), inner)?.as_ref(), 0),
        DataType::Struct(fields) => {
            let array = input.as_any().downcast_ref::<StructArray>().unwrap();
            let mut output = BTreeMap::new();
            for (field, column) in fields.iter().zip(array.columns()) {
                output.insert(field.name().clone(), serialize(column.as_ref(), row)?);
            }
            object(output)
        }
        DataType::Map(_, _) => {
            let entries = input
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap()
                .value(row);
            let mut output = BTreeMap::new();
            let key_nulls = entries.column(0).logical_nulls();
            for index in 0..entries.len() {
                if key_nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)) {
                    return Err(error("JSON map keys cannot be NULL"));
                }
                let key = array_value_to_string(entries.column(0).as_ref(), index)?;
                output.insert(key, serialize(entries.column(1).as_ref(), index)?);
            }
            object(output)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            let field = match input.data_type() {
                DataType::List(f)
                | DataType::LargeList(f)
                | DataType::FixedSizeList(f, _)
                | DataType::ListView(f)
                | DataType::LargeListView(f) => Arc::clone(f),
                _ => unreachable!(),
            };
            let array = cast(&input.slice(row, 1), &DataType::List(field))?;
            let values = array.as_any().downcast_ref::<ListArray>().unwrap().value(0);
            (0..values.len())
                .map(|i| serialize(values.as_ref(), i))
                .collect::<Result<Vec<_>>>()
                .map(|values| format!("[{}]", values.join(",")))
        }
        DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => {
            let value = cast(&input.slice(row, 1), &DataType::Binary)?;
            quoted(
                &base64::engine::general_purpose::STANDARD.encode(
                    value
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                ),
            )
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Ok(array_value_to_string(input, row)?),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let value = cast(&input.slice(row, 1), &DataType::Float64)?;
            if !value
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .is_finite()
            {
                return Err(error(
                    "Non-finite values cannot be represented as JSON numbers",
                ));
            }
            Ok(array_value_to_string(input, row)?)
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => quoted(&array_value_to_string(input, row)?),
        other => Err(error(format!("to_json does not support {other}"))),
    }
}
