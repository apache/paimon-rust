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
        if path == self.original {
            return Ok((self.mounted.clone(), String::new()));
        }
        if let Some(relative) = path
            .strip_prefix(&self.original)
            .and_then(|p| p.strip_prefix('/'))
        {
            return Ok((self.mounted.clone(), relative.into()));
        }
        self.fallback.create(path).await
    }
}

impl FileIO {
    /// Route one URI prefix to a local directory, preserving original paths in
    /// metadata, listings and REST requests. Only component-boundary matches
    /// are translated; unrelated paths retain their existing storage provider.
    /// The caller resolves and validates the mount before supplying this map.
    pub(crate) fn with_fuse(self, original: &str, local: &str) -> crate::Result<Self> {
        let original = original.trim_end_matches('/');
        if original.is_empty() || !std::path::Path::new(local).is_absolute() {
            return Err(Error::ConfigInvalid {
                message: "FUSE requires a nonempty original prefix and an absolute local directory"
                    .into(),
            });
        }
        let mut cfg = opendal_service_fs::FsConfig::default();
        cfg.root = Some(local.into());
        let mounted = Operator::from_config(cfg)?;
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
