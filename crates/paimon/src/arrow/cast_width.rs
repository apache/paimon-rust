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

//! Java character/byte widths shared by casts and authorization transforms.

use arrow_array::{Array, ArrayRef};
use std::sync::Arc;

pub(crate) fn apply_declared_width(array: &ArrayRef, to: &crate::spec::DataType) -> ArrayRef {
    use crate::spec::DataType as Paimon;
    match to {
        Paimon::VarChar(t) => fit_strings(array, t.length() as usize, false),
        Paimon::Char(t) => fit_strings(array, t.length(), true),
        Paimon::VarBinary(t) => fit_bytes(array, t.length() as usize, false),
        Paimon::Binary(t) => fit_bytes(array, t.length(), true),
        _ => Arc::clone(array),
    }
}

/// Counts characters, as Java does.
fn fit_strings(array: &ArrayRef, length: usize, pad: bool) -> ArrayRef {
    let Some(strings) = array.as_any().downcast_ref::<arrow_array::StringArray>() else {
        return Arc::clone(array);
    };
    let fitted: arrow_array::StringArray = strings
        .iter()
        .map(|value| {
            value.map(|value| {
                let chars = value.chars().count();
                if chars > length {
                    value.chars().take(length).collect::<String>()
                } else if pad && chars < length {
                    format!("{value}{}", " ".repeat(length - chars))
                } else {
                    value.to_string()
                }
            })
        })
        .collect();
    Arc::new(fitted)
}

fn fit_bytes(array: &ArrayRef, length: usize, pad: bool) -> ArrayRef {
    let Some(bytes) = array.as_any().downcast_ref::<arrow_array::BinaryArray>() else {
        return Arc::clone(array);
    };
    let fitted: arrow_array::BinaryArray = bytes
        .iter()
        .map(|value| {
            value.map(|value| {
                let mut value = value.to_vec();
                if value.len() > length || pad {
                    value.resize(length, 0);
                }
                value
            })
        })
        .collect();
    Arc::new(fitted)
}
