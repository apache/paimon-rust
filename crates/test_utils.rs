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

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::de::DeserializeOwned;

pub(crate) fn write_int_parquet_file(
    path: &Path,
    columns: Vec<(&str, Vec<i32>)>,
    max_row_group_size: Option<usize>,
) {
    let schema = Arc::new(ArrowSchema::new(
        columns
            .iter()
            .map(|(name, _)| ArrowField::new(*name, ArrowDataType::Int32, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<Arc<dyn Array>> = columns
        .iter()
        .map(|(_, values)| Arc::new(Int32Array::from(values.clone())) as Arc<dyn Array>)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let props = max_row_group_size.map(|size| {
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(size))
            .build()
    });
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, props).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

pub(crate) fn local_file_path(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    if normalized.starts_with('/') {
        format!("file:{normalized}")
    } else {
        format!("file:/{normalized}")
    }
}

pub(crate) fn test_data_file<T>(file_name: &str, row_count: i64, file_size: i64) -> T
where
    T: DeserializeOwned,
{
    serde_json::from_value(serde_json::json!({
        "_FILE_NAME": file_name,
        "_FILE_SIZE": file_size,
        "_ROW_COUNT": row_count,
        "_MIN_KEY": [],
        "_MAX_KEY": [],
        "_KEY_STATS": {
            "_MIN_VALUES": [],
            "_MAX_VALUES": [],
            "_NULL_COUNTS": []
        },
        "_VALUE_STATS": {
            "_MIN_VALUES": [],
            "_MAX_VALUES": [],
            "_NULL_COUNTS": []
        },
        "_MIN_SEQUENCE_NUMBER": 0,
        "_MAX_SEQUENCE_NUMBER": 0,
        "_SCHEMA_ID": 0,
        "_LEVEL": 1,
        "_EXTRA_FILES": [],
        "_CREATION_TIME": Utc::now().timestamp_millis(),
        "_DELETE_ROW_COUNT": null,
        "_EMBEDDED_FILE_INDEX": null,
        "_FILE_SOURCE": null,
        "_VALUE_STATS_COLS": null,
        "_FIRST_ROW_ID": null,
        "_WRITE_COLS": null,
        "_EXTERNAL_PATH": null
    }))
    .unwrap()
}
