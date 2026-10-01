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

//! Validate inline Blob values before any physical file is created.

use crate::spec::{BlobDescriptor, BlobViewStruct, CoreOptions};
use crate::{Error, Result};
use arrow_array::{LargeBinaryArray, RecordBatch};
use std::collections::HashMap;

pub(super) fn validate_inline_blob_columns(
    batch: &RecordBatch,
    options: &HashMap<String, String>,
) -> Result<()> {
    if !options.contains_key("blob-descriptor-field") && !options.contains_key("blob-view-field") {
        return Ok(());
    }
    let options = CoreOptions::new(options);
    for (option, fields) in [
        ("blob-descriptor-field", options.blob_descriptor_fields()),
        ("blob-view-field", options.blob_view_fields()),
    ] {
        for field in fields {
            // A partial-column update may not contain this field.
            let Some(column) = batch.column_by_name(&field) else {
                continue;
            };
            let invalid = |message: String| Error::DataInvalid {
                message: format!("{option} '{field}': {message}"),
                source: None,
            };
            let column = column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| invalid("requires a LargeBinaryArray".into()))?;
            for value in column.iter().flatten() {
                if option == "blob-descriptor-field" {
                    // Schema-declared descriptors use Java's prefix parser;
                    // unlike detection in raw payloads, trailing bytes are valid.
                    BlobDescriptor::deserialize(value).map_err(|error| {
                        invalid(format!("requires a serialized BlobDescriptor: {error}"))
                    })?;
                } else {
                    if !BlobViewStruct::is_blob_view_struct(value) {
                        return Err(invalid("requires a serialized BlobViewStruct".into()));
                    }
                    let view = BlobViewStruct::deserialize(value)
                        .map_err(|error| invalid(error.to_string()))?;
                    if view.serialize()?.as_slice() != value {
                        return Err(invalid("non-canonical BlobViewStruct payload".into()));
                    }
                }
            }
        }
    }
    Ok(())
}
