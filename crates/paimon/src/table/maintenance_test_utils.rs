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

//! Storage I/O counters for automatic maintenance regression tests.

use crate::io::{FileIOBuilder, FileIOProvider};
use crate::spec::{ManifestList, Snapshot, TableSchema};
use crate::table::Table;
use crate::Result;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(super) struct MetadataIo {
    operator: opendal::Operator,
    watched: Mutex<BTreeMap<String, usize>>,
}
#[async_trait::async_trait]
impl FileIOProvider for MetadataIo {
    async fn create(&self, path: &str) -> Result<(opendal::Operator, String)> {
        if let Some(reads) = self.watched.lock().unwrap().get_mut(path) {
            *reads += 1;
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("memory:/")
                .unwrap()
                .trim_start_matches('/')
                .into(),
        ))
    }
}
impl MetadataIo {
    pub fn watch(&self, paths: Vec<String>) {
        *self.watched.lock().unwrap() = paths.into_iter().map(|path| (path, 0)).collect();
    }
    pub fn counts(&self) -> BTreeMap<String, usize> {
        self.watched.lock().unwrap().clone()
    }
}
pub(super) async fn table(schema: TableSchema) -> (Table, Arc<MetadataIo>) {
    let counter = Arc::new(MetadataIo {
        operator: opendal::Operator::from_config(opendal::services::MemoryConfig::default())
            .unwrap(),
        watched: Mutex::new(BTreeMap::new()),
    });
    let io = FileIOBuilder::new("memory")
        .with_provider(counter.clone())
        .build()
        .unwrap();
    let path = format!("memory:/maintenance-io/{}", uuid::Uuid::new_v4());
    let table = Table::new(
        io,
        crate::catalog::Identifier::new("default", "maintenance"),
        path.clone(),
        schema,
        None,
    );
    for directory in ["schema", "snapshot"] {
        table
            .file_io()
            .mkdirs(&format!("{path}/{directory}"))
            .await
            .unwrap();
    }
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(table.schema().id()))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    (table, counter)
}
pub(super) async fn source_manifests(table: &Table, snapshot: &Snapshot) -> Vec<String> {
    let manager = table.snapshot_manager();
    let mut paths = Vec::new();
    for list in [
        snapshot.base_manifest_list(),
        snapshot.delta_manifest_list(),
    ] {
        paths.push(manager.manifest_path(list));
        for manifest in ManifestList::read(table.file_io(), &manager.manifest_path(list))
            .await
            .unwrap()
        {
            paths.push(manager.manifest_path(manifest.file_name()));
        }
    }
    paths
}
