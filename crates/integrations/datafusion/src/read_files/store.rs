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

use async_trait::async_trait;
use cap_std::fs::Dir;
use datafusion::object_store::{
    self, path::Path, Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use paimon::io::{FileIO, FileRead, FileStatus};
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

pub(super) fn failure(
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> object_store::Error {
    object_store::Error::Generic {
        store: "ReadFiles",
        source: source.into(),
    }
}
fn io_error(path: &Path, source: std::io::Error) -> object_store::Error {
    if source.kind() == std::io::ErrorKind::NotFound {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(source),
        }
    } else {
        failure(source)
    }
}
fn read_only() -> object_store::Error {
    object_store::Error::NotSupported {
        source: "read_files source is read-only".into(),
    }
}
#[derive(Debug)]
enum Backend {
    Local(Arc<Dir>),
    FileIO(FileIO),
}

/// An immutable source binding. The caller's runtime registry is never changed.
#[derive(Debug)]
pub(super) struct FileStore {
    root: Url,
    prefix: Path,
    backend: Backend,
}
impl std::fmt::Display for FileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReadFiles({})", self.root)
    }
}
impl FileStore {
    pub(super) fn local(location: Url) -> datafusion::error::Result<Self> {
        let path = location
            .to_file_path()
            .map_err(|_| super::error("invalid local directory"))?;
        let directory = Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
        let prefix = Path::from_url_path(location.path())?;
        let mut root = location;
        root.set_path("/");
        Ok(Self {
            root,
            prefix,
            backend: Backend::Local(Arc::new(directory)),
        })
    }
    pub(super) fn file_io(location: Url, io: FileIO) -> datafusion::error::Result<Self> {
        let prefix = Path::from_url_path(location.path())?;
        let mut root = location;
        root.set_path("/");
        Ok(Self {
            root,
            prefix,
            backend: Backend::FileIO(io),
        })
    }
    fn relative(&self, path: &Path) -> object_store::Result<PathBuf> {
        let parts = path
            .prefix_match(&self.prefix)
            .ok_or_else(|| failure("path is outside the registered location"))?;
        let path: PathBuf = parts.map(|part| part.as_ref().to_string()).collect();
        Ok(if path.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            path
        })
    }
    fn meta(&self, status: FileStatus) -> object_store::Result<ObjectMeta> {
        let memory_root = format!("memory:/{}/", self.root.host_str().unwrap_or(""));
        let path = status
            .path
            .strip_prefix(self.root.as_str())
            .or_else(|| {
                (self.root.scheme() == "memory")
                    .then(|| status.path.strip_prefix(&memory_root))
                    .flatten()
            })
            .ok_or_else(|| failure("FileIO returned a path outside the store"))?;
        let location = Path::parse(path)?;
        self.relative(&location)?;
        Ok(ObjectMeta {
            location,
            size: status.size,
            last_modified: status.last_modified.unwrap_or_default(),
            e_tag: None,
            version: None,
        })
    }
    async fn local_get(
        directory: Arc<Dir>,
        relative: PathBuf,
        location: Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        tokio::task::spawn_blocking(move || {
            // Resolve every component beneath the already-open root and retain
            // this file handle through the actual range reads.
            let mut open_options = cap_std::fs::OpenOptions::new();
            open_options.read(true);
            #[cfg(unix)]
            {
                use cap_std::fs::OpenOptionsExt;
                // Opening a FIFO must not wait for a writer before fstat can
                // reject it. This also covers a concurrent file-type swap.
                open_options.custom_flags(libc::O_NONBLOCK);
            }
            let file = directory
                .open_with(&relative, &open_options)
                .map_err(|e| io_error(&location, e))?
                .into_std();
            let metadata = file.metadata().map_err(|e| io_error(&location, e))?;
            if !metadata.is_file() {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "path is not a regular file".into(),
                });
            }
            let meta = ObjectMeta {
                location: location.clone(),
                size: metadata.len(),
                last_modified: metadata.modified().map_err(failure)?.into(),
                e_tag: None,
                version: None,
            };
            options.check_preconditions(&meta)?;
            let range = match options.range {
                Some(range) => range.as_range(meta.size).map_err(failure)?,
                None => 0..meta.size,
            };
            let payload = if options.head {
                GetResultPayload::Stream(stream::empty().boxed())
            } else {
                GetResultPayload::File(file, relative)
            };
            Ok(GetResult {
                payload,
                meta,
                range,
                attributes: Attributes::new(),
            })
        })
        .await
        .map_err(failure)?
    }
}
#[async_trait]
impl ObjectStore for FileStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let relative = self.relative(location)?;
        if options.version.is_some() {
            return Err(read_only());
        }
        let Backend::FileIO(io) = &self.backend else {
            let Backend::Local(directory) = &self.backend else {
                unreachable!()
            };
            return Self::local_get(directory.clone(), relative, location.clone(), options).await;
        };
        let uri = format!("{}{location}", self.root);
        let input = io.new_input(&uri).map_err(failure)?;
        if !input.exists().await.map_err(failure)? {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "file does not exist".into(),
            });
        }
        let status = input.metadata().await.map_err(failure)?;
        if status.is_dir {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "path is a directory".into(),
            });
        }
        let meta = self.meta(status)?;
        options.check_preconditions(&meta)?;
        let range = match options.range {
            Some(range) => range.as_range(meta.size).map_err(failure)?,
            None => 0..meta.size,
        };
        let payload = if options.head {
            GetResultPayload::Stream(stream::empty().boxed())
        } else {
            let reader = input.reader().await.map_err(failure)?;
            let source =
                stream::try_unfold((reader, range.clone()), |(reader, range)| async move {
                    if range.is_empty() {
                        return Ok(None);
                    }
                    let end = range.start.saturating_add(1024 * 1024).min(range.end);
                    let bytes = reader.read(range.start..end).await.map_err(failure)?;
                    if bytes.len() as u64 != end - range.start {
                        return Err(failure("short FileIO read"));
                    }
                    Ok(Some((bytes, (reader, end..range.end))))
                });
            GetResultPayload::Stream(source.boxed())
        };
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes: Attributes::new(),
        })
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_else(|| self.prefix.clone());
        let relative = match self.relative(&prefix) {
            Ok(path) => path,
            Err(e) => return stream::once(async { Err(e) }).boxed(),
        };
        match &self.backend {
            Backend::FileIO(io) => {
                let io = io.clone();
                let root = self.root.clone();
                let bound = self.prefix.clone();
                stream::once(async move {
                    let statuses = io
                        .list_status_recursive(&format!("{}{}/", root, prefix))
                        .await
                        .map_err(failure)?;
                    let store = FileStore {
                        root,
                        prefix: bound,
                        backend: Backend::FileIO(io),
                    };
                    let files = statuses
                        .into_iter()
                        .filter(|s| !s.is_dir)
                        .map(|s| store.meta(s))
                        .collect::<object_store::Result<Vec<_>>>()?;
                    Ok::<_, object_store::Error>(stream::iter(files.into_iter().map(Ok)))
                })
                .try_flatten()
                .boxed()
            }
            Backend::Local(directory) => {
                let directory = directory.clone();
                let base = self.prefix.clone();
                stream::once(async move {
                    let files = tokio::task::spawn_blocking(move || {
                        let mut pending = vec![relative];
                        let mut files = Vec::new();
                        while let Some(path) = pending.pop() {
                            for entry in directory.read_dir(&path).map_err(failure)? {
                                let entry = entry.map_err(failure)?;
                                let kind = entry.file_type().map_err(failure)?;
                                // Discovery never follows links. Directory swaps
                                // are also constrained by the root capability.
                                if kind.is_symlink() {
                                    continue;
                                }
                                let relative = path.join(entry.file_name());
                                if kind.is_dir() {
                                    pending.push(relative);
                                    continue;
                                }
                                if !kind.is_file() {
                                    continue;
                                }
                                let metadata = directory.metadata(&relative).map_err(failure)?;
                                let mut location = base.clone();
                                for part in relative.components() {
                                    if let std::path::Component::Normal(part) = part {
                                        location =
                                            location.join(part.to_str().ok_or_else(|| {
                                                failure("non-UTF-8 local filename")
                                            })?);
                                    }
                                }
                                files.push(ObjectMeta {
                                    location,
                                    size: metadata.len(),
                                    last_modified: metadata
                                        .modified()
                                        .map_err(failure)?
                                        .into_std()
                                        .into(),
                                    e_tag: None,
                                    version: None,
                                });
                            }
                        }
                        Ok::<_, object_store::Error>(files)
                    })
                    .await
                    .map_err(failure)??;
                    Ok::<_, object_store::Error>(stream::iter(files.into_iter().map(Ok)))
                })
                .try_flatten()
                .boxed()
            }
        }
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        Err(read_only())
    }
    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only())
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(read_only())
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations.map(|_| Err(read_only())).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::object_store::ObjectStoreExt;

    #[tokio::test]
    async fn file_io_ranges_are_bounded_and_read_only() {
        let io = paimon::io::FileIOBuilder::new("memory").build().unwrap();
        io.new_output("memory://ranges/allowed/data")
            .unwrap()
            .write(b"first\nsecond\n".to_vec().into())
            .await
            .unwrap();
        let store =
            FileStore::file_io(Url::parse("memory://ranges/allowed/").unwrap(), io).unwrap();
        assert_eq!(
            store
                .get_range(&Path::from("allowed/data"), 1..4)
                .await
                .unwrap()
                .as_ref(),
            b"irs"
        );
        assert!(store
            .put(&Path::from("allowed/new"), "write".into())
            .await
            .is_err());
        assert!(store.delete(&Path::from("allowed/data")).await.is_err());
        assert!(store.get(&Path::from("private/data")).await.is_err());
        assert!(store
            .list(Some(&Path::from("private")))
            .try_collect::<Vec<_>>()
            .await
            .is_err());
    }
}
