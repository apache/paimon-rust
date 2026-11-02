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

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryBuilder, FixedSizeListArray, Int32Array, Int64Array, LargeListArray,
    ListArray, ListBuilder, MapArray, StringArray, StringBuilder,
};
use datafusion::arrow::compute::{
    cast,
    kernels::temporal::{date_part, DatePart},
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Split,
    Locate,
    Hex,
    Unhex,
    Typeof,
    Size,
    Hour,
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    kind: Kind,
    signature: Signature,
}
pub(crate) fn register(ctx: &SessionContext) {
    for kind in [
        Kind::Split,
        Kind::Locate,
        Kind::Hex,
        Kind::Unhex,
        Kind::Typeof,
        Kind::Size,
        Kind::Hour,
    ] {
        let signature = match kind {
            Kind::Split | Kind::Locate => Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
            Kind::Unhex => Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            Kind::Typeof => Signature::any(1, Volatility::Immutable),
            _ => Signature::user_defined(Volatility::Immutable),
        };
        ctx.register_udf(ScalarUDF::from(Function { kind, signature }));
    }
}
fn decoded_type(mut ty: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    ty
}
fn error(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
impl ScalarUDFImpl for Function {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Split => "split",
            Kind::Locate => "locate",
            Kind::Hex => "hex",
            Kind::Unhex => "unhex",
            Kind::Typeof => "typeof",
            Kind::Size => "size",
            Kind::Hour => "hour",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [ty] = types else {
            return Err(error("Expected one argument"));
        };
        let ty = decoded_type(ty);
        let output = match self.kind {
            Kind::Hex
                if matches!(
                    ty,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
                ) =>
            {
                DataType::Utf8
            }
            Kind::Hex if ty.is_binary() => DataType::Binary,
            Kind::Hex if ty.is_integer() => ty.clone(),
            Kind::Size
                if matches!(
                    ty,
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::Map(_, _)
                        | DataType::Null
                ) =>
            {
                ty.clone()
            }
            Kind::Hour
                if matches!(
                    ty,
                    DataType::Date32
                        | DataType::Date64
                        | DataType::Timestamp(_, _)
                        | DataType::Time32(_)
                        | DataType::Time64(_)
                ) =>
            {
                ty.clone()
            }
            Kind::Hour
                if matches!(
                    ty,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
                ) =>
            {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported {} input type: {ty}",
                    self.name()
                )))
            }
        };
        Ok(vec![output])
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.kind {
            Kind::Split => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            Kind::Locate | Kind::Size | Kind::Hour => DataType::Int32,
            Kind::Unhex => DataType::Binary,
            _ => DataType::Utf8,
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.kind == Kind::Typeof {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(sql_type(
                &args.args[0].data_type(),
            )))));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let output: ArrayRef = match self.kind {
            Kind::Hour => {
                if matches!(arrays[0].data_type(), DataType::Date32 | DataType::Date64) {
                    Arc::new(Int32Array::from_iter(
                        (0..arrays[0].len()).map(|i| (!arrays[0].is_null(i)).then_some(0)),
                    ))
                } else {
                    Arc::new(date_part(arrays[0].as_ref(), DatePart::Hour)?)
                }
            }
            Kind::Size => {
                let a = &arrays[0];
                let values = (0..a.len())
                    .map(|i| {
                        if a.is_null(i) {
                            return Ok(None);
                        }
                        let n = match a.data_type() {
                            DataType::List(_) => {
                                a.as_any()
                                    .downcast_ref::<ListArray>()
                                    .unwrap()
                                    .value_length(i) as i64
                            }
                            DataType::LargeList(_) => a
                                .as_any()
                                .downcast_ref::<LargeListArray>()
                                .unwrap()
                                .value_length(i),
                            DataType::FixedSizeList(_, _) => {
                                a.as_any()
                                    .downcast_ref::<FixedSizeListArray>()
                                    .unwrap()
                                    .value_length() as i64
                            }
                            DataType::Map(_, _) => {
                                a.as_any()
                                    .downcast_ref::<MapArray>()
                                    .unwrap()
                                    .value_length(i) as i64
                            }
                            DataType::Null => return Ok(None),
                            _ => return Err(error("size expects an array or map")),
                        };
                        i32::try_from(n)
                            .map(Some)
                            .map_err(|_| error("Collection size exceeds INT range"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(Int32Array::from(values))
            }
            Kind::Split => {
                let text = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let patterns = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
                let mut output = ListBuilder::new(StringBuilder::new());
                let mut cache = HashMap::new();
                for i in 0..text.len() {
                    if arrays.iter().any(|a| a.is_null(i)) {
                        output.append(false);
                        continue;
                    }
                    let pattern = patterns.value(i);
                    if !cache.contains_key(pattern) {
                        cache.insert(
                            pattern,
                            Regex::new(pattern)
                                .map_err(|e| error(&format!("Invalid split regex: {e}")))?,
                        );
                    }
                    let limit = arrays
                        .get(2)
                        .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                        .unwrap_or(-1);
                    let value = text.value(i);
                    if pattern.is_empty() && !value.is_empty() {
                        let mut start = 0;
                        for (count, (offset, ch)) in value.char_indices().enumerate() {
                            if limit > 0 && count as i64 >= limit - 1 {
                                break;
                            }
                            output.values().append_value(ch.to_string());
                            start = offset + ch.len_utf8();
                        }
                        if start < value.len() {
                            output.values().append_value(&value[start..]);
                        }
                    } else {
                        let mut start = 0;
                        let mut count = 0;
                        for m in cache[pattern].find_iter(value) {
                            if m.start() == 0 && m.end() == 0 {
                                continue;
                            }
                            if limit > 0 && count >= limit - 1 {
                                break;
                            }
                            output.values().append_value(&value[start..m.start()]);
                            start = m.end();
                            count += 1;
                        }
                        output.values().append_value(&value[start..]);
                    }
                    output.append(true);
                }
                Arc::new(output.finish())
            }
            Kind::Locate => {
                let needle = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let haystack = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
                let mut values = Vec::new();
                for i in 0..needle.len() {
                    if arrays.iter().any(|a| a.is_null(i)) {
                        values.push(None);
                        continue;
                    }
                    let start = arrays
                        .get(2)
                        .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                        .unwrap_or(1);
                    let text = haystack.value(i);
                    let offset = if start > 0 {
                        text.char_indices()
                            .map(|(i, _)| i)
                            .chain(std::iter::once(text.len()))
                            .nth((start - 1) as usize)
                    } else {
                        None
                    };
                    let position = offset
                        .and_then(|offset| {
                            text[offset..]
                                .find(needle.value(i))
                                .map(|p| text[..offset + p].chars().count() + 1)
                        })
                        .unwrap_or(0);
                    values.push(Some(
                        i32::try_from(position)
                            .map_err(|_| error("locate result exceeds INT range"))?,
                    ));
                }
                Arc::new(Int32Array::from(values))
            }
            Kind::Unhex => {
                let text = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let mut output = BinaryBuilder::new();
                for value in text.iter() {
                    let Some(value) = value else {
                        output.append_null();
                        continue;
                    };
                    if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        output.append_null();
                        continue;
                    }
                    let value = if value.len() % 2 == 1 {
                        format!("0{value}")
                    } else {
                        value.to_string()
                    };
                    let decoded = (0..value.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&value[i..i + 2], 16))
                        .collect::<std::result::Result<Vec<_>, _>>();
                    match decoded {
                        Ok(bytes) => output.append_value(bytes),
                        Err(_) => output.append_null(),
                    }
                }
                Arc::new(output.finish())
            }
            Kind::Hex => {
                let mut output = StringBuilder::new();
                for i in 0..arrays[0].len() {
                    if arrays[0].is_null(i) {
                        output.append_null();
                        continue;
                    }
                    let scalar = ScalarValue::try_from_array(arrays[0].as_ref(), i)?;
                    let text = if let Some(Some(s)) = scalar.try_as_str() {
                        s.as_bytes()
                            .iter()
                            .map(|b| format!("{b:02X}"))
                            .collect::<String>()
                    } else if let ScalarValue::Binary(Some(bytes)) = scalar {
                        bytes.iter().map(|b| format!("{b:02X}")).collect::<String>()
                    } else {
                        let value = cast(&arrays[0].slice(i, 1), &DataType::UInt64)?;
                        let unsigned = value
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                            .unwrap();
                        if unsigned.is_null(0) {
                            let value = cast(&arrays[0].slice(i, 1), &DataType::Int64)?;
                            let signed = value.as_any().downcast_ref::<Int64Array>().unwrap();
                            format!("{:X}", signed.value(0))
                        } else {
                            format!("{:X}", unsigned.value(0))
                        }
                    };
                    output.append_value(text);
                }
                Arc::new(output.finish())
            }
            Kind::Typeof => unreachable!(),
        };
        if args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)))
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
fn sql_type(ty: &DataType) -> String {
    if decoded_type(ty) == &paimon::arrow::variant_arrow_type() {
        return "variant".into();
    }
    match decoded_type(ty) {
        DataType::Null => "void".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        ty if ty.is_binary() => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, None) => "timestamp_ntz".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp".into(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array<{}>", sql_type(f.data_type()))
        }
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                format!(
                    "map<{},{}>",
                    sql_type(fields[0].data_type()),
                    sql_type(fields[1].data_type())
                )
            } else {
                ty.to_string()
            }
        }
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), sql_type(f.data_type())))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string().to_lowercase(),
    }
}
