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

mod common;

use common::incremental_helpers::{make_batch, memory_table, pk_schema, setup_dirs};

#[tokio::test]
async fn dynamic_bucket_limit_survives_writer_restart() {
    let (io, table) = memory_table(
        "memory:/dynamic_limit",
        pk_schema(&[
            ("bucket", "-1"),
            ("dynamic-bucket.target-row-num", "1"),
            ("dynamic-bucket.max-buckets", "1"),
        ]),
    );
    setup_dirs(&io, table.location()).await;
    for ids in [vec![1, 2, 3], vec![3, 4, 5]] {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(ids.clone(), ids))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 0);
        builder.new_commit().commit(messages).await.unwrap();
    }
}

#[tokio::test]
async fn dynamic_write_rejects_data_without_complete_hash_index() {
    let (io, table) = memory_table(
        "memory:/dynamic_missing",
        pk_schema(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]),
    );
    setup_dirs(&io, table.location()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for message in &mut messages {
        message.new_index_files.clear();
    }
    builder.new_commit().commit(messages).await.unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    let error = writer
        .write_arrow_batch(&make_batch(vec![2], vec![200]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("complete HASH index"), "{error}");
    assert!(writer
        .prepare_commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("cannot be reused"));
    assert!(writer
        .write_arrow_batch(&make_batch(vec![3], vec![30]))
        .await
        .unwrap_err()
        .to_string()
        .contains("cannot be reused"));
}

#[test]
fn dynamic_write_rejects_invalid_max_buckets() {
    for value in ["0", "-2", "32769", "2147483648", "invalid"] {
        let (_, table) = memory_table(
            "memory:/dynamic_invalid",
            pk_schema(&[("bucket", "-1"), ("dynamic-bucket.max-buckets", value)]),
        );
        let result = table.new_write_builder().new_write();
        assert!(result.is_err(), "accepted max-buckets={value}");
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("dynamic-bucket.max-buckets"));
    }
}

#[test]
fn dynamic_write_rejects_custom_bucket_keys() {
    for key in ["id", ""] {
        let (_, table) = memory_table(
            "memory:/dynamic_key",
            pk_schema(&[("bucket", "-1"), ("bucket-key", key)]),
        );
        let error = table.new_write_builder().new_write().err().unwrap();
        assert!(error.to_string().contains("Cannot define 'bucket-key'"));
    }
}

/// Inject failure after at least one bucket HASH was written, independently of
/// HashMap iteration order. Record physical data/index paths to verify ownership.
#[derive(Debug)]
struct FailSecondHash {
    operator: opendal::Operator,
    indexes: std::sync::Mutex<Vec<String>>,
    data: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl paimon::io::FileIOProvider for FailSecondHash {
    async fn create(&self, path: &str) -> paimon::Result<(opendal::Operator, String)> {
        let path = path.strip_prefix("file://").unwrap_or(path);
        let name = path.rsplit('/').next().unwrap();
        if name.starts_with("index-") && path.contains("/index/") {
            let mut indexes = self.indexes.lock().unwrap();
            if !indexes.iter().any(|item| item == path) {
                indexes.push(path.to_string());
            }
            if indexes.get(1).is_some_and(|item| item == path) {
                return Err(paimon::Error::DataInvalid {
                    message: "injected HASH write failure".into(),
                    source: None,
                });
            }
        }
        if name.ends_with(".parquet") {
            self.data.lock().unwrap().push(path.to_string());
        }
        Ok((
            self.operator.clone(),
            path.trim_start_matches('/').to_string(),
        ))
    }
}

#[tokio::test]
async fn failed_hash_prepare_cleans_indexes_and_data() {
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::table::Table;
    use std::sync::Arc;

    let tmp = tempfile::tempdir().unwrap();
    let mut config = opendal_service_fs::FsConfig::default();
    config.root = Some("/".into());
    let provider = Arc::new(FailSecondHash {
        operator: opendal::Operator::from_config(config).unwrap(),
        indexes: Default::default(),
        data: Default::default(),
    });
    let io = FileIOBuilder::new("file")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let path = tmp.path().to_str().unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "failure"),
        path.into(),
        pk_schema(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]),
        None,
    );
    setup_dirs(&io, path).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let error = writer.prepare_commit().await.unwrap_err();
    assert!(
        error.to_string().contains("injected HASH write failure"),
        "{error}"
    );
    let indexes = provider.indexes.lock().unwrap().clone();
    assert_eq!(indexes.len(), 2);
    assert!(indexes
        .iter()
        .all(|path| !std::path::Path::new(path).exists()));
    let data = provider.data.lock().unwrap().clone();
    assert!(!data.is_empty());
    assert!(data.iter().all(|path| !std::path::Path::new(path).exists()));
    assert!(writer
        .prepare_commit()
        .await
        .unwrap_err()
        .to_string()
        .contains("cannot be reused"));
}

#[tokio::test]
async fn failed_later_prepare_preserves_returned_and_committed_files() {
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::table::Table;
    use std::sync::Arc;

    for publish_first in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = opendal_service_fs::FsConfig::default();
        config.root = Some("/".into());
        let provider = Arc::new(FailSecondHash {
            operator: opendal::Operator::from_config(config).unwrap(),
            indexes: Default::default(),
            data: Default::default(),
        });
        let io = FileIOBuilder::new("file")
            .with_provider(provider.clone())
            .build()
            .unwrap();
        let path = tmp.path().to_str().unwrap();
        let table = Table::new(
            io.clone(),
            Identifier::new("default", "later_failure"),
            path.into(),
            pk_schema(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]),
            None,
        );
        setup_dirs(&io, path).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let returned = provider
            .data
            .lock()
            .unwrap()
            .iter()
            .chain(provider.indexes.lock().unwrap().iter())
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        assert!(!returned.is_empty());
        if publish_first {
            builder.new_commit().commit(messages.clone()).await.unwrap();
        }
        writer
            .write_arrow_batch(&make_batch(vec![2], vec![20]))
            .await
            .unwrap();
        assert!(writer
            .prepare_commit()
            .await
            .unwrap_err()
            .to_string()
            .contains("injected HASH write failure"));
        writer.close().await;
        for file in provider
            .data
            .lock()
            .unwrap()
            .iter()
            .chain(provider.indexes.lock().unwrap().iter())
        {
            assert_eq!(
                std::path::Path::new(file).exists(),
                returned.contains(file),
                "{file}"
            );
        }
        if !publish_first {
            builder.new_commit().commit(messages).await.unwrap();
        }
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.total_record_count(), Some(1));
    }
}
