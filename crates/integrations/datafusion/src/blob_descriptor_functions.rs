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
    Array, BinaryArray, BinaryBuilder, BinaryViewArray, LargeBinaryArray, LargeStringArray,
    StringArray, StringBuilder, StringViewArray,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::types::logical_binary;
use datafusion::common::utils::take_function_args;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion::prelude::SessionContext;
use paimon::spec::BlobDescriptor;

use crate::error::to_datafusion_error;

const PATH_TO_DESCRIPTOR: &str = "path_to_descriptor";
const DESCRIPTOR_TO_STRING: &str = "descriptor_to_string";

pub(crate) fn register_blob_descriptor_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(PathToDescriptorFunc::new()));
    ctx.register_udf(ScalarUDF::from(DescriptorToStringFunc::new()));
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathToDescriptorFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl PathToDescriptorFunc {
    fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
            aliases: vec!["sys.path_to_descriptor".to_string()],
        }
    }
}

impl ScalarUDFImpl for PathToDescriptorFunc {
    fn name(&self) -> &str {
        PATH_TO_DESCRIPTOR
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [input] = take_function_args(self.name(), args.args)?;
        match input {
            ColumnarValue::Scalar(value) => path_scalar(value),
            ColumnarValue::Array(array) => path_array(array.as_ref()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DescriptorToStringFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl DescriptorToStringFunc {
    fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_binary(),
                ))],
                Volatility::Immutable,
            ),
            aliases: vec!["sys.descriptor_to_string".to_string()],
        }
    }
}

impl ScalarUDFImpl for DescriptorToStringFunc {
    fn name(&self) -> &str {
        DESCRIPTOR_TO_STRING
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(ArrowDataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let [input] = take_function_args(self.name(), args.args)?;
        match input {
            ColumnarValue::Scalar(value) => descriptor_scalar(value),
            ColumnarValue::Array(array) => descriptor_array(array.as_ref()),
        }
    }
}

fn serialize_path(path: &str) -> Vec<u8> {
    BlobDescriptor::new(path.to_string(), 0, -1).serialize()
}

fn descriptor_string(bytes: &[u8]) -> DFResult<String> {
    BlobDescriptor::deserialize(bytes)
        .map(|descriptor| descriptor.to_string())
        .map_err(to_datafusion_error)
}

fn path_scalar(value: ScalarValue) -> DFResult<ColumnarValue> {
    let path = match value {
        ScalarValue::Utf8(path) | ScalarValue::LargeUtf8(path) | ScalarValue::Utf8View(path) => {
            path
        }
        ScalarValue::Null => None,
        other => return unexpected_type(PATH_TO_DESCRIPTOR, &other.data_type()),
    };
    Ok(ColumnarValue::Scalar(ScalarValue::Binary(
        path.as_deref().map(serialize_path),
    )))
}

fn path_array(input: &dyn Array) -> DFResult<ColumnarValue> {
    if let Some(values) = input.as_any().downcast_ref::<StringArray>() {
        return Ok(descriptor_array_from_paths(values.iter()));
    }
    if let Some(values) = input.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(descriptor_array_from_paths(values.iter()));
    }
    if let Some(values) = input.as_any().downcast_ref::<StringViewArray>() {
        return Ok(descriptor_array_from_paths(values.iter()));
    }
    unexpected_type(PATH_TO_DESCRIPTOR, input.data_type())
}

fn descriptor_array_from_paths<'a>(paths: impl Iterator<Item = Option<&'a str>>) -> ColumnarValue {
    let mut builder = BinaryBuilder::new();
    for path in paths {
        match path {
            Some(path) => builder.append_value(serialize_path(path)),
            None => builder.append_null(),
        }
    }
    ColumnarValue::Array(Arc::new(builder.finish()))
}

fn descriptor_scalar(value: ScalarValue) -> DFResult<ColumnarValue> {
    let bytes = match value {
        ScalarValue::Binary(bytes)
        | ScalarValue::LargeBinary(bytes)
        | ScalarValue::BinaryView(bytes) => bytes,
        ScalarValue::Null => None,
        other => return unexpected_type(DESCRIPTOR_TO_STRING, &other.data_type()),
    };
    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
        bytes.as_deref().map(descriptor_string).transpose()?,
    )))
}

fn descriptor_array(input: &dyn Array) -> DFResult<ColumnarValue> {
    if let Some(values) = input.as_any().downcast_ref::<BinaryArray>() {
        return strings_from_descriptors(values.iter());
    }
    if let Some(values) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        return strings_from_descriptors(values.iter());
    }
    if let Some(values) = input.as_any().downcast_ref::<BinaryViewArray>() {
        return strings_from_descriptors(values.iter());
    }
    unexpected_type(DESCRIPTOR_TO_STRING, input.data_type())
}

fn strings_from_descriptors<'a>(
    descriptors: impl Iterator<Item = Option<&'a [u8]>>,
) -> DFResult<ColumnarValue> {
    let mut builder = StringBuilder::new();
    for bytes in descriptors {
        match bytes {
            Some(bytes) => builder.append_value(descriptor_string(bytes)?),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn unexpected_type<T>(function: &str, data_type: &ArrowDataType) -> DFResult<T> {
    Err(DataFusionError::Execution(format!(
        "{function} received unexpected argument type {data_type}"
    )))
}
