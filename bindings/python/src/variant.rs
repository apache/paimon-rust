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

use arrow::array::{make_array, new_empty_array, Array, ArrayData, StructArray};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::error::to_py_err;

/// Extract a path with PyPaimon's exact Arrow type semantics, without casts.
#[pyfunction]
fn variant_get(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    path: &str,
    data_type: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let pa = py.import("pyarrow")?;
    if !data_type.is_instance(&pa.getattr("DataType")?)? {
        return Err(PyTypeError::new_err(
            "VARIANT data_type must be a PyArrow data type",
        ));
    }
    let types = pa.getattr("types")?;
    let target = representable_target(&pa, &types, data_type)?;
    let data = import_variant_array(&pa, &types, column)?;
    data.validate_full()
        .map_err(|error| PyValueError::new_err(format!("Invalid Arrow Variant input: {error}")))?;
    let column = make_array(data);
    let output = py
        .detach(|| {
            let column = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| paimon::Error::DataInvalid {
                    message: "Expected a PyArrow Variant StructArray".to_string(),
                    source: None,
                })?;
            paimon::arrow::variant_get(column, path, &target)
        })
        .map_err(|error| match error {
            paimon::Error::Unsupported { .. } | paimon::Error::DataTypeInvalid { .. } => {
                PyTypeError::new_err(error.to_string())
            }
            _ => to_py_err(error),
        })?;
    // Field names containing NUL cannot cross C strings. Export a schema with
    // safe names, then restore all original annotations using the same buffers.
    let array = FFI_ArrowArray::new(&output.to_data());
    let schema = FFI_ArrowSchema::try_from(&transport_type(output.data_type()))
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let output = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (&raw const array as usize, &raw const schema as usize),
    )?;
    Ok(restore_target(&pa, &types, &output, data_type)?.unbind())
}

/// Check logical field identities before C strings can truncate names, and keep
/// opaque input annotations outside Arrow Rust's UTF8-only metadata transport.
fn import_variant_array(
    pa: &Bound<'_, PyModule>,
    types: &Bound<'_, PyAny>,
    column: &Bound<'_, PyAny>,
) -> PyResult<ArrayData> {
    if !column.is_instance(&pa.getattr("Array")?)? {
        return Err(PyTypeError::new_err(
            "VARIANT input must be a PyArrow Array",
        ));
    }
    let data_type = column.getattr("type")?;
    if !types
        .call_method1("is_struct", (&data_type,))?
        .is_truthy()?
    {
        return Err(PyValueError::new_err(
            "Expected a PyArrow Variant StructArray",
        ));
    }
    let fields = data_type.try_iter()?.collect::<PyResult<Vec<_>>>()?;
    if fields.len() != 2
        || fields[0].getattr("name")?.extract::<String>()? != "value"
        || fields[1].getattr("name")?.extract::<String>()? != "metadata"
    {
        return Err(PyValueError::new_err(
            "Expected Variant struct fields value and metadata",
        ));
    }
    for (index, field) in fields.iter().enumerate() {
        let mut data_type = field.getattr("type")?;
        if index == 1
            && types
                .call_method1("is_dictionary", (&data_type,))?
                .is_truthy()?
        {
            if DataType::from_pyarrow_bound(&data_type.getattr("index_type")?)? != DataType::Int32 {
                return Err(PyValueError::new_err(
                    "Variant metadata dictionary must use Int32 keys",
                ));
            }
            data_type = data_type.getattr("value_type")?;
        }
        if !types
            .call_method1("is_binary", (&data_type,))?
            .is_truthy()?
            && !types
                .call_method1("is_large_binary", (&data_type,))?
                .is_truthy()?
        {
            return Err(PyValueError::new_err(
                "Variant child must be Binary or LargeBinary",
            ));
        }
    }
    if column.len()? == 0 {
        // Empty slices may retain nonzero Binary offsets while the C interface
        // omits their unused payload buffers. Canonical empty children avoid
        // validating those offsets against zero-length imported buffers.
        return Ok(new_empty_array(&DataType::Struct(
            vec![
                Field::new("value", DataType::Binary, true),
                Field::new("metadata", DataType::Binary, true),
            ]
            .into(),
        ))
        .to_data());
    }
    // Always rewrap: a sliced StructArray can export unsliced children. Select
    // its visible child ranges and reset the parent offset before validation.
    let children = (0..2)
        .map(|index| column.call_method1("field", (index,)))
        .collect::<PyResult<Vec<_>>>()?;
    let fields = fields
        .iter()
        .map(|field| field.call_method0("remove_metadata"))
        .collect::<PyResult<Vec<_>>>()?;
    let kwargs = PyDict::new(pa.py());
    kwargs.set_item("fields", PyList::new(pa.py(), fields)?)?;
    kwargs.set_item("mask", column.call_method0("is_null")?)?;
    let clean = pa.getattr("StructArray")?.call_method(
        "from_arrays",
        (PyList::new(pa.py(), children)?,),
        Some(&kwargs),
    )?;
    ArrayData::from_pyarrow_bound(&clean)
}

/// Adapt logical schema metadata that Arrow Rust cannot represent. No Variant
/// value is converted: stored decimal scales never exceed 38, so a target scale
/// above 127 can only match nulls. Reject extensions before their storage types
/// lose their logical identity at the Rust DataType boundary.
fn representable_target<'py>(
    pa: &Bound<'py, PyModule>,
    types: &Bound<'py, PyAny>,
    target: &Bound<'py, PyAny>,
) -> PyResult<DataType> {
    if target.is_instance(&pa.getattr("BaseExtensionType")?)? {
        return Err(PyTypeError::new_err(format!(
            "Unsupported exact VARIANT data type: {target}"
        )));
    }
    let is_type = |name| types.call_method1(name, (target,))?.is_truthy();
    if is_type("is_decimal128")? {
        let scale = target.getattr("scale")?.extract::<i32>()?;
        if scale < 0 {
            return Err(PyValueError::new_err(
                "VARIANT decimal scale must be non-negative",
            ));
        }
        return Ok(DataType::Decimal128(
            target.getattr("precision")?.extract()?,
            scale.min(i8::MAX as i32) as i8,
        ));
    }
    let field = |field: Bound<'py, PyAny>| {
        let data_type = representable_target(pa, types, &field.getattr("type")?)?;
        // Names are Rust strings, not C strings. Field metadata remains on the
        // Python target because it can contain arbitrary, non-UTF8 bytes.
        Ok::<_, PyErr>(Arc::new(Field::new(
            field.getattr("name")?.extract::<String>()?,
            data_type,
            field.getattr("nullable")?.extract::<bool>()?,
        )))
    };
    if is_type("is_struct")? {
        let fields = target
            .try_iter()?
            .map(|item| field(item?))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(DataType::Struct(fields.into()));
    }
    if is_type("is_list")? {
        return Ok(DataType::List(field(target.getattr("value_field")?)?));
    }
    if is_type("is_large_list")? {
        return Ok(DataType::LargeList(field(target.getattr("value_field")?)?));
    }
    if is_type("is_fixed_size_list")? {
        return Ok(DataType::FixedSizeList(
            field(target.getattr("value_field")?)?,
            target.getattr("list_size")?.extract()?,
        ));
    }
    if is_type("is_map")? {
        return Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        field(target.getattr("key_field")?)?,
                        field(target.getattr("item_field")?)?,
                    ]
                    .into(),
                ),
                false,
            )),
            target.getattr("keys_sorted")?.extract()?,
        ));
    }
    DataType::from_pyarrow_bound(target)
}

fn transport_type(data_type: &DataType) -> DataType {
    let field = |field: &FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(field.name().replace('\0', ""))
                .with_data_type(transport_type(field.data_type())),
        )
    };
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::List(child) => DataType::List(field(child)),
        DataType::LargeList(child) => DataType::LargeList(field(child)),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(field(child), *size),
        DataType::Map(entries, ordered) => DataType::Map(field(entries), *ordered),
        _ => data_type.clone(),
    }
}

/// Rebuild only Arrow array wrappers, never values or buffers. Unlike casts or
/// views, from_buffers retains nonnullable annotations even when children are
/// missing/null, and preserves custom map field names and opaque metadata.
fn restore_target<'py>(
    pa: &Bound<'py, PyModule>,
    types: &Bound<'py, PyAny>,
    array: &Bound<'py, PyAny>,
    target: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let is_type = |name| types.call_method1(name, (target,))?.is_truthy();
    let mut children = Vec::new();
    if is_type("is_struct")? {
        for (index, field) in target.try_iter()?.enumerate() {
            children.push(restore_target(
                pa,
                types,
                &array.call_method1("field", (index,))?,
                &field?.getattr("type")?,
            )?);
        }
    } else if is_type("is_list")? || is_type("is_large_list")? || is_type("is_fixed_size_list")? {
        children.push(restore_target(
            pa,
            types,
            &array.getattr("values")?,
            &target.getattr("value_type")?,
        )?);
    } else if is_type("is_map")? {
        children.push(restore_target(
            pa,
            types,
            &array.getattr("values")?,
            &target.call_method1("field", (0,))?.getattr("type")?,
        )?);
    }
    let buffers = array.call_method0("buffers")?;
    let buffers = buffers
        .cast::<PyList>()?
        .get_slice(0, target.getattr("num_buffers")?.extract()?);
    let kwargs = PyDict::new(pa.py());
    kwargs.set_item("children", PyList::new(pa.py(), children)?)?;
    kwargs.set_item("null_count", array.getattr("null_count")?)?;
    kwargs.set_item("offset", array.getattr("offset")?)?;
    pa.getattr("Array")?.call_method(
        "from_buffers",
        (target, array.len()?, buffers),
        Some(&kwargs),
    )
}

/// Extract literal top-level numeric Variant fields into a row-major array.
#[pyfunction]
fn variant_get_numeric_fields(
    py: Python<'_>,
    column: &Bound<'_, PyAny>,
    fields: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let column = make_array(ArrayData::from_pyarrow_bound(column)?);
    let output = py
        .detach(|| {
            let column = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| paimon::Error::DataInvalid {
                    message: "Expected a PyArrow Variant StructArray".to_string(),
                    source: None,
                })?;
            paimon::arrow::variant_get_numeric_fields(column, &fields)
        })
        .map_err(to_py_err)?;
    Ok(output.to_data().to_pyarrow(py)?.unbind())
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "data")?;
    this.add_function(wrap_pyfunction!(variant_get, &this)?)?;
    this.add_function(wrap_pyfunction!(variant_get_numeric_fields, &this)?)?;
    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pypaimon_rust.data", this)?;
    Ok(())
}
