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

use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF as DFScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

fn parse_arrow_type(type_name: &str) -> PyResult<ArrowDataType> {
    match type_name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(ArrowDataType::Boolean),
        "int8" => Ok(ArrowDataType::Int8),
        "int16" => Ok(ArrowDataType::Int16),
        "int" | "int32" | "integer" => Ok(ArrowDataType::Int32),
        "bigint" | "int64" | "long" => Ok(ArrowDataType::Int64),
        "float" | "float32" => Ok(ArrowDataType::Float32),
        "double" | "float64" => Ok(ArrowDataType::Float64),
        "string" | "utf8" => Ok(ArrowDataType::Utf8),
        "large_string" | "large_utf8" => Ok(ArrowDataType::LargeUtf8),
        "binary" => Ok(ArrowDataType::Binary),
        "large_binary" => Ok(ArrowDataType::LargeBinary),
        other => Err(PyTypeError::new_err(format!(
            "Unsupported Arrow type for Python UDF: {other}"
        ))),
    }
}

fn parse_arrow_type_like(value: &Bound<'_, PyAny>) -> PyResult<ArrowDataType> {
    if let Ok(field) = ArrowField::from_pyarrow_bound(value) {
        return Ok(field.data_type().clone());
    }
    if let Ok(data_type) = ArrowDataType::from_pyarrow_bound(value) {
        return Ok(data_type);
    }
    if let Ok(type_name) = value.extract::<String>() {
        return parse_arrow_type(&type_name);
    }

    Err(PyTypeError::new_err(
        "Expected a pyarrow.DataType, pyarrow.Field, or supported Arrow type name",
    ))
}

fn parse_input_types(input_fields: &Bound<'_, PyAny>) -> PyResult<Vec<ArrowDataType>> {
    if let Ok(fields) = input_fields.cast::<PyList>() {
        return fields
            .iter()
            .map(|field| parse_arrow_type_like(&field))
            .collect();
    }
    if let Ok(fields) = input_fields.cast::<PyTuple>() {
        return fields
            .iter()
            .map(|field| parse_arrow_type_like(&field))
            .collect();
    }

    Ok(vec![parse_arrow_type_like(input_fields)?])
}

fn parse_volatility(volatility: &Bound<'_, PyAny>) -> PyResult<Volatility> {
    let value = if let Ok(value) = volatility.extract::<String>() {
        value
    } else if let Ok(name) = volatility.getattr("name") {
        name.extract::<String>()?
    } else {
        volatility.str()?.to_str()?.to_string()
    };

    match value.to_ascii_lowercase().as_str() {
        "immutable" => Ok(Volatility::Immutable),
        "stable" => Ok(Volatility::Stable),
        "volatile" => Ok(Volatility::Volatile),
        other => Err(PyTypeError::new_err(format!(
            "Unsupported UDF volatility: {other}. Expected immutable, stable, or volatile"
        ))),
    }
}

fn sanitize_udf_name(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();

    if sanitized.is_empty() {
        sanitized.push_str("python_udf");
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert(0, '_');
    }
    sanitized
}

fn default_udf_name(py: Python<'_>, func: &Py<PyAny>) -> PyResult<String> {
    let func = func.bind(py);
    if let Ok(name) = func.getattr("__name__") {
        return Ok(sanitize_udf_name(&name.extract::<String>()?));
    }
    if let Ok(name) = func.getattr("__qualname__") {
        return Ok(sanitize_udf_name(&name.extract::<String>()?));
    }
    let name = func
        .getattr("__class__")?
        .getattr("__name__")?
        .extract::<String>()?;
    Ok(sanitize_udf_name(&name))
}

fn df_execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn columnar_value_to_array(value: &ColumnarValue, num_rows: usize) -> DFResult<ArrayRef> {
    match value {
        ColumnarValue::Array(array) => Ok(Arc::clone(array)),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
    }
}

struct PyScalarUDF {
    name: String,
    func: Py<PyAny>,
    return_type: ArrowDataType,
    signature: Signature,
}

impl PyScalarUDF {
    fn new(
        name: String,
        func: Py<PyAny>,
        return_type: ArrowDataType,
        signature: Signature,
    ) -> Self {
        Self {
            name,
            func,
            return_type,
            signature,
        }
    }
}

pub(crate) fn build_python_scalar_udf(
    name: String,
    func: Py<PyAny>,
    return_type: ArrowDataType,
    signature: Signature,
) -> DFScalarUDF {
    DFScalarUDF::new_from_impl(PyScalarUDF::new(name, func, return_type, signature))
}

#[pyclass(name = "PythonScalarUDF")]
pub struct PyPythonScalarUDFObject {
    name: String,
    udf: DFScalarUDF,
}

impl PyPythonScalarUDFObject {
    fn create(
        py: Python<'_>,
        name: String,
        func: Py<PyAny>,
        input_fields: &Bound<'_, PyAny>,
        return_field: &Bound<'_, PyAny>,
        volatility: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        if !func.bind(py).is_callable() {
            return Err(PyTypeError::new_err("`func` argument must be callable"));
        }

        let input_types = parse_input_types(input_fields)?;
        let return_type = parse_arrow_type_like(return_field)?;
        let volatility = parse_volatility(volatility)?;
        let signature = Signature::exact(input_types, volatility);
        let udf = PyScalarUDF::new(name.clone(), func, return_type, signature);
        Ok(Self {
            name,
            udf: DFScalarUDF::new_from_impl(udf),
        })
    }

    pub(crate) fn datafusion_udf(&self) -> DFScalarUDF {
        self.udf.clone()
    }
}

#[pymethods]
impl PyPythonScalarUDFObject {
    #[new]
    fn new(
        py: Python<'_>,
        name: String,
        func: Py<PyAny>,
        input_fields: Bound<'_, PyAny>,
        return_field: Bound<'_, PyAny>,
        volatility: Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Self::create(py, name, func, &input_fields, &return_field, &volatility)
    }

    #[staticmethod]
    #[pyo3(signature = (func, input_fields, return_field, volatility, name = None))]
    fn udf(
        py: Python<'_>,
        func: Py<PyAny>,
        input_fields: Bound<'_, PyAny>,
        return_field: Bound<'_, PyAny>,
        volatility: Bound<'_, PyAny>,
        name: Option<String>,
    ) -> PyResult<Self> {
        let name = match name {
            Some(name) => name,
            None => default_udf_name(py, &func)?,
        };
        Self::create(py, name, func, &input_fields, &return_field, &volatility)
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __repr__(&self) -> String {
        format!("PythonScalarUDF({})", self.name)
    }
}

#[pyfunction]
#[pyo3(signature = (func, input_fields, return_field, volatility, name = None))]
pub(crate) fn udf(
    py: Python<'_>,
    func: Py<PyAny>,
    input_fields: Bound<'_, PyAny>,
    return_field: Bound<'_, PyAny>,
    volatility: Bound<'_, PyAny>,
    name: Option<String>,
) -> PyResult<PyPythonScalarUDFObject> {
    PyPythonScalarUDFObject::udf(py, func, input_fields, return_field, volatility, name)
}

impl Debug for PyScalarUDF {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("return_type", &self.return_type)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PyScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.return_type == other.return_type
            && self.signature == other.signature
    }
}

impl Eq for PyScalarUDF {}

impl Hash for PyScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.return_type.hash(state);
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for PyScalarUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DFResult<ArrowDataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|value| columnar_value_to_array(value, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let output = Python::try_attach(|py| -> PyResult<ArrayRef> {
            let py_args = arrays
                .iter()
                .map(|array| array.to_data().to_pyarrow(py))
                .collect::<PyResult<Vec<_>>>()?;
            let py_args = PyTuple::new(py, py_args)?;
            let output = self.func.bind(py).call1(py_args)?;
            Ok(make_array(ArrayData::from_pyarrow_bound(&output)?))
        })
        .ok_or_else(|| df_execution_error("Python interpreter is not available"))?
        .map_err(|err| df_execution_error(format!("Python UDF '{}' failed: {err}", self.name)))?;

        if output.len() != args.number_rows {
            return Err(df_execution_error(format!(
                "Python UDF '{}' returned {} rows, expected {}",
                self.name,
                output.len(),
                args.number_rows
            )));
        }
        if output.data_type() != &self.return_type {
            return Err(df_execution_error(format!(
                "Python UDF '{}' returned {:?}, expected {:?}",
                self.name,
                output.data_type(),
                self.return_type
            )));
        }

        Ok(ColumnarValue::Array(output))
    }
}
