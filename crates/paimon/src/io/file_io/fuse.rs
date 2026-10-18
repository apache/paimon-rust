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

//! Access original table URIs through a resolved FUSE mount.

use super::*;

#[derive(Debug)]
struct FuseFileIO {
    original: String,
    mounted: Operator,
    fallback: FileIO,
}

#[async_trait::async_trait]
impl FileIOProvider for FuseFileIO {
    async fn create(&self, path: &str) -> crate::Result<(Operator, String)> {
        if let Some(relative) = self.relative_path(path) {
            return Ok((self.mounted.clone(), relative.into()));
        }
        self.fallback.create(path).await
    }

    async fn create_with_cache_namespace(
        &self,
        path: &str,
    ) -> crate::Result<(Operator, String, Option<String>)> {
        if let Some(relative) = self.relative_path(path) {
            // FileIO combines this namespace with the operator's actual mount
            // root and relative path, keeping different mounts isolated.
            return Ok((self.mounted.clone(), relative.into(), Some("fuse".into())));
        }
        self.fallback.create_with_cache_namespace(path).await
    }
}

impl FuseFileIO {
    fn relative_path<'a>(&self, path: &'a str) -> Option<&'a str> {
        if path == self.original {
            Some("")
        } else {
            path.strip_prefix(&self.original)
                .and_then(|p| p.strip_prefix('/'))
        }
    }
}

impl FileIO {
    /// Route one URI prefix to a local directory, preserving original paths in
    /// metadata, listings and REST requests. Only component-boundary matches
    /// are translated; unrelated paths retain their existing storage provider.
    /// The caller resolves and validates the mount before supplying this map.
    pub fn with_fuse(self, original: &str, local: &str) -> crate::Result<Self> {
        let original = original.trim_end_matches('/');
        if original.is_empty() || !std::path::Path::new(local).is_absolute() {
            return Err(Error::ConfigInvalid {
                message: "FUSE requires a nonempty original prefix and an absolute local directory"
                    .into(),
            });
        }
        let mounted = super::super::fs_config_build_with_root(local)?;
        let provider = Arc::new(FuseFileIO {
            original: original.into(),
            mounted,
            fallback: self.clone(),
        });
        Ok(self.with_provider(provider))
    }
}

#[cfg(all(test, feature = "storage-memory"))]
mod tests {
    use super::*;

    fn cached_base() -> FileIO {
        let mut options = crate::common::Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_WHITELIST, "*");
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
        let mut base = crate::io::FileIOBuilder::new("memory").build().unwrap();
        base.cache = crate::io::cache::create_local_cache(&options).unwrap();
        base
    }

    #[tokio::test]
    async fn bound_mount_supports_table_write_commit_and_truncate() {
        use crate::catalog::Identifier;
        use crate::spec::{DataType, IntType, Schema, TableSchema};
        use crate::table::{Table, TableCommit};
        use arrow_array::{Int32Array, RecordBatch};
        use futures::TryStreamExt;

        let mount = tempfile::tempdir().unwrap();
        let root = "oss://bucket/warehouse/table";
        let base = FileIOBuilder::new("memory").build().unwrap();
        let io = base
            .clone()
            .with_fuse(root, mount.path().to_str().unwrap())
            .unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table = Table::new(
            base,
            Identifier::new("db", "table"),
            root.into(),
            TableSchema::new(0, &schema),
            None,
        )
        .copy_with_file_io(io.clone());
        // SchemaManager must follow the replacement FileIO as well as data I/O.
        let schema_path = format!("{root}/schema/schema-0");
        io.new_output(&schema_path)
            .unwrap()
            .write(bytes::Bytes::from(
                serde_json::to_vec(table.schema()).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(table.schema_manager().schema(0).await.unwrap().id(), 0);
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let files = messages
            .iter()
            .flat_map(|m| {
                m.new_files
                    .iter()
                    .map(move |f| format!("{root}/bucket-{}/{}", m.bucket, f.file_name))
            })
            .collect::<Vec<_>>();
        let committer = TableCommit::new(table.clone(), builder.commit_user().into());
        committer.commit_with_identifier(messages, 1).await.unwrap();
        writer.close().await;
        assert!(mount.path().join("snapshot/snapshot-1").exists());
        for file in &files {
            assert!(io.exists(file).await.unwrap());
        }
        let reader = table.new_read_builder();
        let plan = reader.new_scan().plan().await.unwrap();
        let mut batches = reader.new_read().unwrap().to_arrow(plan.splits()).unwrap();
        let mut count = 0;
        while let Some(batch) = batches.try_next().await.unwrap() {
            count += batch.num_rows();
        }
        assert_eq!(count, 2);
        committer.truncate_table_with_identifier(17).await.unwrap();
        assert!(table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap()
            .splits()
            .is_empty());
        // Logical truncation must retain files referenced by older snapshots.
        for file in &files {
            assert!(io.exists(file).await.unwrap());
        }
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        committer
            .commit_with_identifier(writer.prepare_commit().await.unwrap(), 18)
            .await
            .unwrap();
        // Retrying an uncertain truncate cannot remove the later append.
        committer.truncate_table_with_identifier(17).await.unwrap();
        assert!(!table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap()
            .splits()
            .is_empty());
    }

    #[tokio::test]
    async fn mounted_cache_reuses_files_isolates_mounts_and_invalidates_writes() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let base = cached_base();
        let uri = "memory:/table/data.parquet";
        let io = base
            .clone()
            .with_fuse("memory:/table", first.path().to_str().unwrap())
            .unwrap();
        let other = base
            .clone()
            .with_fuse("memory:/table", second.path().to_str().unwrap())
            .unwrap();
        std::fs::write(first.path().join("data.parquet"), "first").unwrap();
        std::fs::write(second.path().join("data.parquet"), "second").unwrap();
        let input = io.new_input(uri).unwrap();
        let reader = input.reader().await.unwrap();
        assert!(
            reader.cache_key().is_some(),
            "mounted readers must have stable file identities"
        );
        let reloaded = base
            .with_fuse("memory:/table", first.path().to_str().unwrap())
            .unwrap();
        assert_eq!(
            reader.cache_key(),
            reloaded
                .new_input(uri)
                .unwrap()
                .reader()
                .await
                .unwrap()
                .cache_key()
        );
        assert_eq!(input.read().await.unwrap(), "first");
        assert_eq!(
            other.new_input(uri).unwrap().read().await.unwrap(),
            "second"
        );
        // Without any source left to read, the second read must use cached blocks.
        std::fs::remove_file(first.path().join("data.parquet")).unwrap();
        assert_eq!(input.read().await.unwrap(), "first");
        io.new_output(uri)
            .unwrap()
            .write(Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(input.read().await.unwrap(), "new");
        let destination = "memory:/table/renamed.parquet";
        io.new_output(destination)
            .unwrap()
            .write(Bytes::from_static(b"old destination"))
            .await
            .unwrap();
        assert_eq!(
            io.new_input(destination).unwrap().read().await.unwrap(),
            "old destination"
        );
        io.rename(uri, destination).await.unwrap();
        assert!(input.read().await.is_err());
        assert_eq!(
            io.new_input(destination).unwrap().read().await.unwrap(),
            "new"
        );
    }

    #[tokio::test]
    async fn unmapped_files_keep_the_fallback_cache_identity() {
        let root = tempfile::tempdir().unwrap();
        let base = cached_base();
        let io = base
            .clone()
            .with_fuse("memory:/table", root.path().to_str().unwrap())
            .unwrap();
        let uri = "memory:/outside/data.parquet";
        base.new_output(uri)
            .unwrap()
            .write(Bytes::from_static(b"fallback"))
            .await
            .unwrap();
        let input = base.new_input(uri).unwrap();
        let key = input
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        assert_eq!(
            io.new_input(uri)
                .unwrap()
                .reader()
                .await
                .unwrap()
                .cache_key(),
            Some(key.as_str())
        );
        assert_eq!(input.read().await.unwrap(), "fallback");
        // Bypass invalidation at the backing service to demonstrate shared caching.
        let (op, path) = base.create_static(uri).unwrap();
        op.delete(&path).await.unwrap();
        assert_eq!(io.new_input(uri).unwrap().read().await.unwrap(), "fallback");
    }

    #[tokio::test]
    async fn mounted_reads_writes_and_lists_keep_the_original_uri() {
        let root = tempfile::tempdir().unwrap();
        let base = crate::io::FileIOBuilder::new("memory").build().unwrap();
        base.new_output("memory:/table-other/kept")
            .unwrap()
            .write(Bytes::from_static(b"remote"))
            .await
            .unwrap();
        let io = base
            .with_fuse("memory:/table/", root.path().to_str().unwrap())
            .unwrap();
        io.new_output("memory:/table/data/a")
            .unwrap()
            .write(Bytes::from_static(b"mounted"))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("data/a")).unwrap(),
            b"mounted"
        );
        assert_eq!(
            io.new_input("memory:/table/data/a")
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"mounted")
        );
        let entries = io.list_status("memory:/table/data").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "memory:/table/data/a");
        assert_eq!(
            io.new_input("memory:/table-other/kept")
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"remote")
        );
        assert!(io
            .new_input("memory:/table/../escape")
            .unwrap()
            .read()
            .await
            .is_err());
        io.delete_file("memory:/table/data/a").await.unwrap();
        assert!(!root.path().join("data/a").exists());
    }
}
