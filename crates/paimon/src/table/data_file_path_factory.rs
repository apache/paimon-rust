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

//! Java-style per-bucket file placement shared by all physical writers.

use super::external_path::ExternalPathProvider;
use crate::spec::{bucket_path_under, data_file_path, relative_bucket_path, CoreOptions};
use crate::Result;
use std::collections::HashMap;
use std::sync::Mutex;

/// Select a location once per new data/changelog/Blob file. Sidecars use the
/// selected file's parent; they must never advance the external-path provider.
pub(super) struct DataFilePathFactory {
    bucket_path: String,
    external: Option<Mutex<ExternalPathProvider>>,
}

pub(super) struct DataFilePath {
    pub path: String,
    pub external_path: Option<String>,
}

impl DataFilePath {
    pub fn parent(&self) -> &str {
        self.path
            .rsplit_once('/')
            .expect("data file has a parent")
            .0
    }
}

impl DataFilePathFactory {
    pub fn new(
        table: &str,
        partition: &str,
        bucket: i32,
        options: &HashMap<String, String>,
    ) -> Result<Self> {
        let options_view = CoreOptions::new(options);
        let directory = options_view.data_file_path_directory();
        let bucket_path = bucket_path_under(&data_file_path(table, directory), partition, bucket);
        let relative = relative_bucket_path(partition, bucket, directory);
        Ok(Self {
            bucket_path,
            external: ExternalPathProvider::new(options, &relative)?.map(Mutex::new),
        })
    }

    pub fn bucket_path(&self) -> &str {
        &self.bucket_path
    }

    pub fn new_path(&self, name: &str) -> Result<DataFilePath> {
        let external_path = self
            .external
            .as_ref()
            .map(|provider| {
                provider
                    .lock()
                    .map_err(|_| crate::Error::UnexpectedError {
                        message: "External path provider lock is poisoned".into(),
                        source: None,
                    })
                    .map(|mut provider| provider.next_path(name))
            })
            .transpose()?;
        let path = external_path
            .clone()
            .unwrap_or_else(|| format!("{}/{name}", self.bucket_path));
        Ok(DataFilePath {
            path,
            external_path,
        })
    }
}
