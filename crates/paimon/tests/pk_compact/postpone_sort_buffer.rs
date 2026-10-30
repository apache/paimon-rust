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

use super::*;
use opendal::Operator;
use paimon::io::{FileIOBuilder, FileIOProvider};
use paimon::resource::ResourceContext;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct StageProbe {
    operator: Operator,
    paths: Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl FileIOProvider for StageProbe {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        if path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("postpone-sort-"))
        {
            self.paths.lock().unwrap().insert(path.to_string());
        }
        Ok((
            self.operator.clone(),
            path.strip_prefix("memory:/").unwrap().to_string(),
        ))
    }
}

async fn probed_table(options: &[(&str, &str)]) -> (Table, Arc<StageProbe>) {
    let probe = Arc::new(StageProbe {
        operator: Operator::from_config(opendal::services::MemoryConfig::default()).unwrap(),
        paths: Mutex::new(HashSet::new()),
    });
    let io = FileIOBuilder::new("memory")
        .with_provider(probe.clone())
        .build()
        .unwrap();
    let mut configured = vec![
        ("bucket", "-2"),
        ("read.batch-size", "1"),
        ("postpone.default-bucket-num", "1"),
    ];
    configured.extend_from_slice(options);
    let table = Table::new(
        io.clone(),
        paimon::catalog::Identifier::new("default", "sort_buffer"),
        format!("memory:/sort_buffer/{}", uuid::Uuid::new_v4()),
        pk_schema(&configured),
        None,
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    (table, probe)
}

#[tokio::test]
async fn postpone_sort_spills_by_buffer_size_instead_of_read_batch_boundaries() {
    for (budget, one_run) in [("4mb", true), ("16kb", false)] {
        let (table, probe) = probed_table(&[("write-buffer-size", budget)]).await;
        write_batch(
            &table,
            &make_batch((0..600).rev().collect(), (0..600).rev().collect()),
        )
        .await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let resources = ResourceContext::builder()
            .memory_limit(16 * 1024 * 1024)
            .build()
            .unwrap();
        let executor = table
            .new_compact_builder()
            .with_resources(resources.clone())
            .new_executor()
            .unwrap();
        let result = executor.execute(&plan.tasks()[0]).await.unwrap();
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        let count = probe.paths.lock().unwrap().len();
        if one_run {
            assert_eq!(count, 1);
        } else {
            assert!((2..600).contains(&count), "unexpected spill count {count}");
        }
        builder
            .new_commit()
            .unwrap()
            .commit_compact(vec![result])
            .await
            .unwrap();
        assert_eq!(
            rows(&table).await,
            (0..600).map(|key| (key, key)).collect::<Vec<_>>()
        );
        assert!(table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .iter()
            .all(|file| !file.path.contains("postpone-sort-")));
    }
}

#[tokio::test]
async fn postpone_sort_buffer_failure_releases_retained_memory_and_creates_no_output() {
    let (table, probe) = probed_table(&[("file.format", "row")]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let resources = ResourceContext::builder().memory_limit(1).build().unwrap();
    let executor = table
        .new_compact_builder()
        .with_resources(resources.clone())
        .new_executor()
        .unwrap();
    let error = executor.execute(&plan.tasks()[0]).await.unwrap_err();
    assert!(
        matches!(error, paimon::Error::ResourceExhausted { .. }),
        "{error}"
    );
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert!(probe.paths.lock().unwrap().is_empty());
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(1)
    );
}
