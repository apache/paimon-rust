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
    Array, ArrayRef, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
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
    SplitIndex,
    Locate,
    Typeof,
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct Function {
    kind: Kind,
    signature: Signature,
}
pub(crate) fn register(ctx: &SessionContext) {
    for kind in [Kind::Split, Kind::SplitIndex, Kind::Locate, Kind::Typeof] {
        let signature = if kind == Kind::Typeof {
            Signature::any(1, Volatility::Immutable)
        } else if kind == Kind::SplitIndex {
            Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            )
        } else {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            )
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
            Kind::SplitIndex => "split_index",
            Kind::Locate => "locate",
            Kind::Typeof => "typeof",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.kind {
            Kind::Split => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            Kind::Locate => DataType::Int32,
            Kind::Typeof | Kind::SplitIndex => DataType::Utf8,
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
            Kind::SplitIndex => {
                let text = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let separators = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
                let indexes = arrays[2].as_any().downcast_ref::<Int64Array>().unwrap();
                let output = (0..text.len())
                    .map(|i| {
                        if arrays.iter().any(|a| a.is_null(i)) || text.value(i).is_empty() {
                            return None;
                        }
                        let index = usize::try_from(indexes.value(i)).ok()?;
                        let separator = separators.value(i);
                        if separator.is_empty() {
                            text.value(i).split(java_whitespace).nth(index)
                        } else {
                            text.value(i).split(separator).nth(index)
                        }
                    })
                    .collect::<StringArray>();
                Arc::new(output)
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
// StringUtils uses Java Character.isWhitespace when the whole separator is empty.
fn java_whitespace(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}' |
        '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
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
