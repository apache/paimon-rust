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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(super) struct WatchedReads {
    pub started: Semaphore,
    pub release: Semaphore,
}

#[derive(Debug)]
pub(super) struct MetadataIo {
    operator: opendal::Operator,
    watched: Mutex<BTreeMap<String, usize>>,
    observed: Mutex<BTreeSet<String>>,
    pause: Mutex<Option<Arc<WatchedReads>>>,
}
#[async_trait::async_trait]
impl FileIOProvider for MetadataIo {
    async fn create(&self, path: &str) -> Result<(opendal::Operator, String)> {
        self.observed.lock().unwrap().insert(path.into());
        let watched = self
            .watched
            .lock()
            .unwrap()
            .get_mut(path)
            .is_some_and(|reads| {
                *reads += 1;
                true
            });
        let pause = self.pause.lock().unwrap().clone();
        if let Some(pause) = pause.filter(|_| watched) {
            pause.started.add_permits(1);
            pause.release.acquire().await.unwrap().forget();
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
    pub fn pause_watched_reads(&self) -> Arc<WatchedReads> {
        let pause = Arc::new(WatchedReads {
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        *self.pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    pub fn watch(&self, paths: Vec<String>) {
        *self.watched.lock().unwrap() = paths.into_iter().map(|path| (path, 0)).collect();
    }
    pub fn counts(&self) -> BTreeMap<String, usize> {
        self.watched.lock().unwrap().clone()
    }
    pub fn observed_paths(&self) -> BTreeSet<String> {
        self.observed.lock().unwrap().clone()
    }
}
pub(super) async fn table(schema: TableSchema) -> (Table, Arc<MetadataIo>) {
    let counter = Arc::new(MetadataIo {
        operator: opendal::Operator::from_config(opendal::services::MemoryConfig::default())
            .unwrap(),
        watched: Mutex::new(BTreeMap::new()),
        observed: Mutex::new(BTreeSet::new()),
        pause: Mutex::new(None),
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
