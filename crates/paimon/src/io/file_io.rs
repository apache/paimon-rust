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

use crate::common::options::{parse_memory_size, CatalogOptions};
use crate::error::*;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use opendal::raw::{normalize_path, normalize_root};
use opendal::Operator;
use snafu::ResultExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use url::Url;

use super::cache::{CachedFileReader, LocalCache};
use super::Storage;

#[cfg(all(test, feature = "storage-memory"))]
mod provider_tests;

/// An externally managed block cache used by [`FileIO`].
///
/// Implementations store immutable file ranges. Cache failures must be handled
/// as misses/no-ops so the storage backend remains the source of truth.
#[async_trait::async_trait]
pub trait FileBlockCache: std::fmt::Debug + Send + Sync + 'static {
    /// Return the exact requested range on a hit, or `None` on a miss.
    async fn get(&self, path: &str, range: Range<u64>) -> Option<Bytes>;

    /// Store a range. Implementations may silently decline the write.
    async fn put(&self, path: &str, offset: u64, data: Bytes);

    /// Remove all cached ranges for one file.
    async fn invalidate_path(&self, path: &str);

    /// Remove all cached ranges under a directory prefix.
    async fn invalidate_prefix(&self, prefix: &str);
}

/// Resolves original paths to application-managed OpenDAL operators.
///
/// Providers own scheme/bucket routing, operator reuse, and credential refresh.
/// Errors propagate without falling back to built-in storage or credentials.
/// Already-open readers and writers retain their operator, so its backend must
/// refresh credentials internally if those handles need to outlive credentials.
///
/// Custom services must report distinct `(scheme, name, root)` storage identities
/// for different namespaces, since these identities are used by the file cache.
#[async_trait::async_trait]
pub trait FileIOProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Return an operator and its relative path, preserving literal object keys.
    ///
    /// Empty paths and `/` denote the operator root. For directory listings, the
    /// relative path must be an unchanged suffix of the original path starting
    /// at a component boundary; this permits reconstruction of reusable full URIs.
    /// Object paths that OpenDAL would trim or collapse are rejected by FileIO.
    /// Rename requires both paths to resolve to the same shared service instance.
    async fn create(&self, path: &str) -> crate::Result<(Operator, String)>;
}

#[derive(Clone, Debug)]
enum FileIOBackend {
    Storage(Arc<Storage>),
    Provider(Arc<dyn FileIOProvider>),
}

#[derive(Clone)]
pub struct FileIO {
    backend: FileIOBackend,
    cache: Option<Arc<LocalCache>>,
    context_id: u64,
    file_format_metadata_cache_max_bytes: usize,
}

pub(crate) const DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_BYTES: usize = 50 * 1024 * 1024;

static NEXT_FILE_IO_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_file_io_context_id() -> u64 {
    NEXT_FILE_IO_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)
}

impl std::fmt::Debug for FileIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIO")
            .field("backend", &self.backend)
            .field("cache", &self.cache)
            .field("context_id", &self.context_id)
            .field(
                "file_format_metadata_cache_max_bytes",
                &self.file_format_metadata_cache_max_bytes,
            )
            .finish()
    }
}

impl FileIO {
    /// Attach an externally managed block cache.
    ///
    /// `block_size` controls the aligned ranges presented to the cache.
    /// `whitelist` uses the same comma-separated values as
    /// `local-cache.whitelist`: `meta`, `global-index`, `bucket-index`, `data`,
    /// and `file-index`.
    pub fn with_file_block_cache(
        mut self,
        cache: Arc<dyn FileBlockCache>,
        block_size: u64,
        whitelist: &str,
    ) -> crate::Result<Self> {
        self.cache = Some(Arc::new(LocalCache::external(
            cache, block_size, whitelist,
        )?));
        Ok(self)
    }

    /// Replace the storage backend with a provider, retaining the file cache.
    ///
    /// Resolution is deferred to async operations, including for file handles
    /// subsequently created by [`Self::new_input`] and [`Self::new_output`].
    pub fn with_provider(mut self, provider: Arc<dyn FileIOProvider>) -> Self {
        self.backend = FileIOBackend::Provider(provider);
        self.context_id = next_file_io_context_id();
        self
    }

    pub(crate) fn create_static(&self, path: &str) -> crate::Result<(Operator, String)> {
        let FileIOBackend::Storage(storage) = &self.backend else {
            return Err(Error::IoUnsupported {
                message: "A FileIOProvider requires async path resolution".to_string(),
            });
        };
        let (op, relative_path) = storage.create(path)?;
        Ok((op, relative_path.into_owned()))
    }

    async fn create(&self, path: &str) -> crate::Result<(Operator, String)> {
        match &self.backend {
            FileIOBackend::Provider(provider) => resolve_provider(provider.as_ref(), path).await,
            FileIOBackend::Storage(_) => self.create_static(path),
        }
    }

    fn file_source(&self, path: &str) -> crate::Result<FileSource> {
        match &self.backend {
            FileIOBackend::Provider(provider) => Ok(FileSource::Provider(provider.clone())),
            FileIOBackend::Storage(_) => {
                let (op, relative_path) = self.create_static(path)?;
                let cache_path = cache_object_path(&op, &relative_path);
                Ok(FileSource::Static {
                    op,
                    relative_path,
                    cache_path,
                })
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn has_local_cache(&self) -> bool {
        self.cache.is_some()
    }

    /// Try to infer file io scheme from path.
    ///
    /// The input HashMap is paimon-java's [`Options`](https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/options/Options.java#L60)
    pub fn from_url(path: &str) -> crate::Result<FileIOBuilder> {
        let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid URL: {path}"),
        })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> crate::Result<FileIOBuilder> {
        let path = path.as_ref();
        let url = if looks_like_windows_drive_path(path) {
            Url::from_file_path(path).map_err(|_| Error::ConfigInvalid {
                message: format!("Input {path} is neither a valid url nor path"),
            })?
        } else {
            Url::parse(path)
                .map_err(|_| Error::ConfigInvalid {
                    message: format!("Invalid URL: {path}"),
                })
                .or_else(|_| {
                    Url::from_file_path(path).map_err(|_| Error::ConfigInvalid {
                        message: format!("Input {path} is neither a valid url nor path"),
                    })
                })?
        };
        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Create a new input file to read data.
    /// With a provider, path resolution and its errors are deferred to async IO.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L76>
    pub fn new_input(&self, path: &str) -> crate::Result<InputFile> {
        Ok(InputFile {
            source: self.file_source(path)?,
            path: path.to_string(),
            context_id: self.context_id,
            file_format_metadata_cache_max_bytes: self.file_format_metadata_cache_max_bytes,
            cache: self
                .cache
                .as_ref()
                .filter(|cache| cache.is_cacheable(path))
                .cloned(),
        })
    }

    /// Create a new output file to write data.
    /// With a provider, path resolution and its errors are deferred to async IO.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L87>
    pub fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile {
            source: self.file_source(path)?,
            path: path.to_string(),
            context_id: self.context_id,
            file_format_metadata_cache_max_bytes: self.file_format_metadata_cache_max_bytes,
            cache: self
                .cache
                .as_ref()
                .filter(|cache| cache.is_cacheable(path))
                .cloned(),
        })
    }

    /// Return a file status object that represents the path.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L97>
    pub async fn get_status(&self, path: &str) -> Result<FileStatus> {
        let (op, relative_path) = self.create(path).await?;
        let meta = op
            .stat(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to get file status for '{path}'"),
            })?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            last_modified: meta
                .last_modified()
                .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
            path: path.to_string(),
        })
    }

    /// List the statuses of the files/directories in the given path if the path is a directory.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L105>
    ///
    /// FIXME: how to handle large dir? Better to return a stream instead?
    pub async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let (op, relative_path) = self.create(path).await?;
        let base_path = listing_base_path(path, &relative_path)?;
        // Opendal list() expects directory path to end with `/`.
        // use normalize_root to make sure it end with `/`.
        let list_path = normalize_root(relative_path.as_ref());

        let entries = op.list_with(&list_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to list files in '{path}'"),
        })?;

        let mut statuses = Vec::new();
        let list_path_normalized = list_path.trim_start_matches('/');
        for entry in entries {
            let entry_path = entry.path();
            if matches!(self.backend, FileIOBackend::Provider(_)) {
                validate_provider_path(path, entry_path)?;
            }
            if entry_path.trim_start_matches('/') == list_path_normalized {
                continue;
            }
            let meta = entry.metadata();
            statuses.push(FileStatus {
                size: meta.content_length(),
                is_dir: meta.is_dir(),
                path: status_path(&base_path, entry_path),
                last_modified: meta
                    .last_modified()
                    .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
            });
        }

        Ok(statuses)
    }

    /// List all files recursively under the given directory path.
    pub async fn list_status_recursive(&self, path: &str) -> Result<Vec<FileStatus>> {
        self.list_status_recursive_with_limit(path, None).await
    }

    pub(crate) async fn list_status_recursive_with_limit(
        &self,
        path: &str,
        limit: Option<usize>,
    ) -> Result<Vec<FileStatus>> {
        self.list_status_recursive_stream(path, limit)
            .await?
            .try_collect()
            .await
    }

    pub(crate) async fn list_status_recursive_stream(
        &self,
        path: &str,
        limit: Option<usize>,
    ) -> Result<BoxStream<'static, Result<FileStatus>>> {
        if limit == Some(0) {
            return Ok(futures::stream::empty().boxed());
        }

        let (op, relative_path) = self.create(path).await?;
        let base_path = listing_base_path(path, &relative_path)?;
        let has_provider = matches!(self.backend, FileIOBackend::Provider(_));
        let list_path = normalize_root(relative_path.as_ref());

        let entries =
            op.lister_with(&list_path)
                .recursive(true)
                .await
                .context(IoUnexpectedSnafu {
                    message: format!("Failed to list files recursively in '{path}'"),
                })?;

        let path = path.to_string();
        let list_path_normalized = list_path.trim_start_matches('/').to_string();
        Ok(Box::pin(async_stream::try_stream! {
            let mut entries = entries;
            let mut emitted = 0usize;
            while let Some(entry) = entries.try_next().await.context(IoUnexpectedSnafu {
                message: format!("Failed to list files recursively in '{path}'"),
            })? {
                let entry_path = entry.path();
                if has_provider {
                    validate_provider_path(&path, entry_path)?;
                }
                if entry_path.trim_start_matches('/') == list_path_normalized {
                    continue;
                }
                let meta = entry.metadata();
                if meta.is_dir() {
                    continue;
                }
                yield FileStatus {
                    size: meta.content_length(),
                    is_dir: false,
                    path: status_path(&base_path, entry_path),
                    last_modified: meta
                        .last_modified()
                        .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
                };
                emitted += 1;
                if limit.is_some_and(|limit| emitted >= limit) {
                    break;
                }
            }
        }))
    }

    /// Check if exists.
    ///
    /// References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L128>
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create(path).await?;

        op.exists(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to check existence of '{path}'"),
            })
    }

    /// Check if a directory exists.
    pub async fn exists_dir(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.create(path).await?;
        let dir_path = normalize_root(relative_path.as_ref());

        op.exists(&dir_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to check existence of directory '{path}'"),
        })
    }

    /// Delete a file.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create(path).await?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());

        op.delete(relative_path.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to delete file '{path}'"),
            })?;
        if let Some(cache) = self.cache.as_ref().filter(|cache| cache.is_cacheable(path)) {
            cache.invalidate_path(&cache_path).await;
        }

        Ok(())
    }

    /// Delete a dir recursively.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L139>
    pub async fn delete_dir(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create(path).await?;
        let cache_path = cache_object_path(&op, relative_path.as_ref());

        op.delete_with(relative_path.as_ref())
            .recursive(true)
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to delete directory '{path}'"),
            })?;
        if let Some(cache) = &self.cache {
            cache.invalidate_prefix(&cache_path).await;
        }

        Ok(())
    }

    /// Make the given file and all non-existent parents into directories.
    ///
    /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L150>
    pub async fn mkdirs(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.create(path).await?;
        // Opendal create_dir expects the path to end with `/` to indicate a directory.
        let dir_path = normalize_root(relative_path.as_ref());
        op.create_dir(&dir_path).await.context(IoUnexpectedSnafu {
            message: format!("Failed to create directory '{path}'"),
        })?;

        Ok(())
    }

    /// Copy a file from src to dst.
    ///
    /// Overwrites dst if it already exists.
    pub async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        let input = self.new_input(src)?;
        let bytes = input.read().await?;
        let output = self.new_output(dst)?;
        output.write(bytes).await?;
        Ok(())
    }

    /// Copy a large file without materializing its entire contents in memory.
    /// Format Table publication uses this when a backend does not support
    /// rename, as is common for object stores and the in-memory test backend.
    pub async fn copy_file_streaming(&self, src: &str, dst: &str) -> Result<()> {
        const CHUNK_SIZE: u64 = 8 * 1024 * 1024;
        let input = self.new_input(src)?;
        let size = input.metadata().await?.size;
        let reader = input.reader().await?;
        let output = self.new_output(dst)?;
        let mut writer = output.writer().await?;
        let mut position = 0;
        while position < size {
            let end = (position + CHUNK_SIZE).min(size);
            let bytes = reader.read(position..end).await?;
            if bytes.len() as u64 != end - position {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Short read while copying '{src}': expected {} bytes, got {}",
                        end - position,
                        bytes.len()
                    ),
                    source: None,
                });
            }
            writer.write(bytes).await?;
            position = end;
        }
        writer.close().await?;
        Ok(())
    }

    /// Renames the file/directory src to dst.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fs/FileIO.java#L159>
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let (op_src, relative_path_src) = self.create(src).await?;
        let (op_dst, relative_path_dst) = self.create(dst).await?;
        if matches!(self.backend, FileIOBackend::Provider(_))
            && !Arc::ptr_eq(op_src.service(), op_dst.service())
        {
            return Err(Error::IoUnsupported {
                message: "Rename through a FileIOProvider requires the same shared storage service"
                    .to_string(),
            });
        }
        let cache_path_src = cache_object_path(&op_src, relative_path_src.as_ref());
        let cache_path_dst = cache_object_path(&op_dst, relative_path_dst.as_ref());

        op_src
            .rename(relative_path_src.as_ref(), relative_path_dst.as_ref())
            .await
            .context(IoUnexpectedSnafu {
                message: format!("Failed to rename '{src}' to '{dst}'"),
            })?;
        if let Some(cache) = &self.cache {
            cache.invalidate_prefix(&cache_path_src).await;
            cache.invalidate_prefix(&cache_path_dst).await;
        }

        Ok(())
    }
}

async fn resolve_provider(
    provider: &dyn FileIOProvider,
    path: &str,
) -> crate::Result<(Operator, String)> {
    let (op, relative_path) = provider.create(path).await?;
    validate_provider_path(path, &relative_path)?;
    Ok((op, relative_path))
}

fn validate_provider_path(path: &str, relative_path: &str) -> Result<()> {
    // Filesystem paths retain their existing separator normalization. Object
    // keys must not silently resolve to another object through OpenDAL's path
    // normalization (e.g. `a//b` -> `a/b` or `key ` -> `key`).
    if path.contains("://")
        && !path.starts_with("file:/")
        && !relative_path.is_empty()
        && normalize_path(relative_path) != relative_path
    {
        return Err(Error::ConfigInvalid {
            message: "FileIOProvider returned an object path that OpenDAL would normalize"
                .to_string(),
        });
    }
    Ok(())
}

fn listing_base_path(path: &str, relative_path: &str) -> Result<String> {
    if relative_path.is_empty() || relative_path == "/" {
        return Ok(path.to_string());
    }
    if let Some(base) = path.strip_suffix(relative_path) {
        if base.is_empty() || base.ends_with('/') {
            return Ok(base.to_string());
        }
    }
    // Windows filesystem paths only change separators. Try the original path
    // first, since backslashes are literal filename characters on POSIX.
    if looks_like_windows_drive_path(path) || (cfg!(windows) && path.starts_with("file:/")) {
        let normalized = path.replace('\\', "/");
        if let Some(base) = normalized.strip_suffix(relative_path) {
            if base.is_empty() || base.ends_with('/') {
                return Ok(base.to_string());
            }
        }
    }
    Err(Error::ConfigInvalid {
        message: "Cannot list a path whose resolved relative path is not a component suffix"
            .to_string(),
    })
}

fn status_path(base_path: &str, entry_path: &str) -> String {
    if base_path.ends_with('/') || entry_path.starts_with('/') {
        format!("{base_path}{entry_path}")
    } else {
        format!("{base_path}/{entry_path}")
    }
}

fn cache_object_path(op: &Operator, relative_path: &str) -> String {
    let info = op.info();
    format!(
        "{}\0{}\0{}\0{}",
        info.scheme(),
        info.name(),
        info.root(),
        relative_path.trim_start_matches('/')
    )
}

/// Whether `path` begins with a Windows drive specifier such as `C:\` or `C:/`.
pub(crate) fn looks_like_windows_drive_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/')
}

#[derive(Debug)]
pub struct FileIOBuilder {
    scheme_str: Option<String>,
    props: HashMap<String, String>,
    cache: Option<Arc<LocalCache>>,
    operator: Option<Operator>,
    provider: Option<Arc<dyn FileIOProvider>>,
}

impl FileIOBuilder {
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
            cache: None,
            operator: None,
            provider: None,
        }
    }

    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>, Option<Operator>) {
        (
            self.scheme_str.unwrap_or_default(),
            self.props,
            self.operator,
        )
    }

    /// Uses a caller-provided opendal operator as a **filesystem** backend instead of building
    /// one from the scheme: embedders bring a customized local-filesystem service without
    /// registering a scheme. Paths are resolved with the local-filesystem rules — absolute
    /// paths, `file:` URLs, and Windows drive paths — and handed to the operator in relative
    /// form, so the operator's root decides what they resolve against. Scheme'd paths
    /// (`s3://…`, `oss://…`) are rejected rather than misresolved: an object-store operator
    /// needs bucket/scheme resolution this hook deliberately does not provide.
    pub fn with_fs_operator(mut self, operator: Operator) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Use an application-managed provider instead of built-in storage.
    ///
    /// The provider receives original paths, regardless of the builder's scheme.
    /// Storage properties are not parsed and no built-in storage feature is
    /// required. Combining this with [`Self::with_fs_operator`] is an error.
    pub fn with_provider(mut self, provider: Arc<dyn FileIOProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    pub(crate) fn with_local_cache(mut self, cache: Arc<LocalCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn build(mut self) -> crate::Result<FileIO> {
        let cache = self.cache.clone();
        let file_format_metadata_cache_max_bytes = self
            .props
            .get(CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE)
            .map(|value| {
                parse_memory_size(value)
                    .ok()
                    .and_then(|bytes| usize::try_from(bytes).ok())
                    .ok_or_else(|| Error::ConfigInvalid {
                        message: format!(
                            "Invalid value for {}: {value}",
                            CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE
                        ),
                    })
            })
            .transpose()?
            .unwrap_or(DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_BYTES);
        let backend = if let Some(provider) = self.provider.take() {
            if self.operator.is_some() {
                return Err(Error::ConfigInvalid {
                    message: "with_provider and with_fs_operator cannot be combined".to_string(),
                });
            }
            FileIOBackend::Provider(provider)
        } else {
            FileIOBackend::Storage(Arc::new(Storage::build(self)?))
        };
        Ok(FileIO {
            backend,
            cache,
            context_id: next_file_io_context_id(),
            file_format_metadata_cache_max_bytes,
        })
    }
}

#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;

    /// Stable identity of an immutable file within one storage context.
    fn cache_key(&self) -> Option<&str> {
        None
    }

    fn file_format_metadata_cache_max_bytes(&self) -> usize {
        DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_BYTES
    }
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

enum InputFileReader {
    Direct {
        reader: opendal::Reader,
        cache_key: String,
        file_format_metadata_cache_max_bytes: usize,
    },
    Cached {
        reader: CachedFileReader,
        cache_key: String,
        file_format_metadata_cache_max_bytes: usize,
    },
}

#[async_trait::async_trait]
impl FileRead for InputFileReader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        match self {
            Self::Direct { reader, .. } => FileRead::read(reader, range).await,
            Self::Cached { reader, .. } => FileRead::read(reader, range).await,
        }
    }

    fn cache_key(&self) -> Option<&str> {
        match self {
            Self::Direct { cache_key, .. } | Self::Cached { cache_key, .. } => Some(cache_key),
        }
    }

    fn file_format_metadata_cache_max_bytes(&self) -> usize {
        match self {
            Self::Direct {
                file_format_metadata_cache_max_bytes,
                ..
            }
            | Self::Cached {
                file_format_metadata_cache_max_bytes,
                ..
            } => *file_format_metadata_cache_max_bytes,
        }
    }
}

#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    async fn close(&mut self) -> crate::Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        opendal::Writer::close(self).await?;
        Ok(())
    }
}

struct CacheInvalidatingWriter {
    delegate: Box<dyn FileWrite>,
    cache: Arc<LocalCache>,
    path: String,
}

#[async_trait::async_trait]
impl FileWrite for CacheInvalidatingWriter {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        self.delegate.write(bs).await
    }

    async fn close(&mut self) -> crate::Result<()> {
        self.delegate.close().await?;
        self.cache.invalidate_path(&self.path).await;
        Ok(())
    }
}

/// Async streaming writer trait for format-level writers (e.g. parquet).
pub trait AsyncFileWrite: tokio::io::AsyncWrite + Unpin + Send {}

impl<T: tokio::io::AsyncWrite + Unpin + Send> AsyncFileWrite for T {}

struct CacheInvalidatingAsyncWriter {
    delegate: Box<dyn AsyncFileWrite>,
    cache: Arc<LocalCache>,
    path: String,
    delegate_shutdown: bool,
    invalidation: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl tokio::io::AsyncWrite for CacheInvalidatingAsyncWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.delegate).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.delegate).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.delegate_shutdown {
            match Pin::new(&mut *self.delegate).poll_shutdown(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    self.delegate_shutdown = true;
                    let cache = self.cache.clone();
                    let path = self.path.clone();
                    self.invalidation =
                        Some(Box::pin(async move { cache.invalidate_path(&path).await }));
                }
            }
        }

        if let Some(invalidation) = &mut self.invalidation {
            match invalidation.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => self.invalidation = None,
            }
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
    pub is_dir: bool,
    pub path: String,
    pub last_modified: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
enum FileSource {
    Static {
        op: Operator,
        relative_path: String,
        cache_path: String,
    },
    Provider(Arc<dyn FileIOProvider>),
}

impl FileSource {
    async fn resolve(&self, path: &str) -> crate::Result<(Operator, String, String)> {
        match self {
            Self::Provider(provider) => {
                let (op, relative_path) = resolve_provider(provider.as_ref(), path).await?;
                let cache_path = cache_object_path(&op, &relative_path);
                Ok((op, relative_path, cache_path))
            }
            Self::Static {
                op,
                relative_path,
                cache_path,
            } => Ok((op.clone(), relative_path.clone(), cache_path.clone())),
        }
    }
}

#[derive(Debug)]
pub struct InputFile {
    source: FileSource,
    path: String,
    context_id: u64,
    file_format_metadata_cache_max_bytes: usize,
    cache: Option<Arc<LocalCache>>,
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        let (op, relative_path, _) = self.source.resolve(&self.path).await?;
        Ok(op.exists(&relative_path).await?)
    }

    pub async fn metadata(&self) -> crate::Result<FileStatus> {
        let (op, relative_path, _) = self.source.resolve(&self.path).await?;
        let meta = op.stat(&relative_path).await?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            path: self.path.clone(),
            last_modified: meta
                .last_modified()
                .map(|v| DateTime::<Utc>::from(SystemTime::from(v))),
        })
    }

    pub async fn read(&self) -> crate::Result<Bytes> {
        let (op, relative_path, cache_path) = self.source.resolve(&self.path).await?;
        let Some(cache) = &self.cache else {
            return Ok(op.read(&relative_path).await?.to_bytes());
        };
        let read_token = cache.read_token(&cache_path);
        let size = if let Some(size) = cache.file_size(&cache_path, &read_token).await {
            size
        } else {
            let size = op.stat(&relative_path).await?.content_length();
            cache.put_file_size(&cache_path, size, &read_token).await;
            size
        };
        let delegate = Arc::new(op.reader(&relative_path).await?);
        CachedFileReader::new_with_token(delegate, &cache_path, size, cache.clone(), read_token)
            .read_full()
            .await
    }

    pub async fn reader(&self) -> crate::Result<impl FileRead> {
        let (op, relative_path, cache_path) = self.source.resolve(&self.path).await?;
        let metadata_cache_key = format!("{}\0{cache_path}", self.context_id);
        let reader = op.reader(&relative_path).await?;
        let Some(cache) = &self.cache else {
            return Ok(InputFileReader::Direct {
                reader,
                cache_key: metadata_cache_key,
                file_format_metadata_cache_max_bytes: self.file_format_metadata_cache_max_bytes,
            });
        };
        let read_token = cache.read_token(&cache_path);
        let size = if let Some(size) = cache.file_size(&cache_path, &read_token).await {
            size
        } else {
            let size = op.stat(&relative_path).await?.content_length();
            cache.put_file_size(&cache_path, size, &read_token).await;
            size
        };
        Ok(InputFileReader::Cached {
            reader: CachedFileReader::new_with_token(
                Arc::new(reader),
                &cache_path,
                size,
                cache.clone(),
                read_token,
            ),
            cache_key: metadata_cache_key,
            file_format_metadata_cache_max_bytes: self.file_format_metadata_cache_max_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OutputFile {
    source: FileSource,
    path: String,
    context_id: u64,
    file_format_metadata_cache_max_bytes: usize,
    cache: Option<Arc<LocalCache>>,
}

impl OutputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> crate::Result<bool> {
        let (op, relative_path, _) = self.source.resolve(&self.path).await?;
        Ok(op.exists(&relative_path).await?)
    }

    pub fn to_input_file(self) -> InputFile {
        let cache = self.cache.filter(|cache| cache.is_cacheable(&self.path));
        InputFile {
            source: self.source,
            path: self.path,
            context_id: self.context_id,
            file_format_metadata_cache_max_bytes: self.file_format_metadata_cache_max_bytes,
            cache,
        }
    }

    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        let (op, relative_path, cache_path) = self.source.resolve(&self.path).await?;
        let writer: Box<dyn FileWrite> = Box::new(
            op.writer_with(&relative_path)
                .chunk(8 * 1024 * 1024)
                .await?,
        );
        let Some(cache) = &self.cache else {
            return Ok(writer);
        };
        Ok(Box::new(CacheInvalidatingWriter {
            delegate: writer,
            cache: cache.clone(),
            path: cache_path,
        }))
    }

    /// Get an async streaming writer for format-level writes (e.g. parquet).
    pub(crate) async fn async_writer(&self) -> crate::Result<Box<dyn AsyncFileWrite>> {
        let (op, relative_path, cache_path) = self.source.resolve(&self.path).await?;
        let writer: Box<dyn AsyncFileWrite> = Box::new(
            op.writer_with(&relative_path)
                .chunk(8 * 1024 * 1024)
                .concurrent(1)
                .await?
                .into_futures_async_write()
                .compat_write(),
        );
        let Some(cache) = &self.cache else {
            return Ok(writer);
        };
        Ok(Box::new(CacheInvalidatingAsyncWriter {
            delegate: writer,
            cache: cache.clone(),
            path: cache_path,
            delegate_shutdown: false,
            invalidation: None,
        }))
    }
}

#[cfg(test)]
mod file_action_test {
    use std::collections::BTreeSet;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::tempdir;

    use super::*;
    use bytes::Bytes;
    use opendal::raw::{
        oio, OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
        RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer,
    };
    use opendal::{Capability, EntryMode, Metadata, OperationContext};

    #[derive(Debug)]
    struct CountingListProvider {
        pulls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FileIOProvider for CountingListProvider {
        async fn create(&self, _path: &str) -> crate::Result<(Operator, String)> {
            let service: Servicer = Arc::new(CountingListService {
                pulls: Arc::clone(&self.pulls),
            });
            Ok((
                Operator::from_parts(OperationContext::default(), service),
                "objects/".to_string(),
            ))
        }
    }

    #[derive(Debug)]
    struct CountingListService {
        pulls: Arc<AtomicUsize>,
    }

    impl Service for CountingListService {
        type Reader = ();
        type Writer = ();
        type Lister = CountingLister;
        type Deleter = ();
        type Copier = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("counting")
        }

        fn capability(&self) -> Capability {
            Capability {
                list: true,
                list_with_recursive: true,
                ..Default::default()
            }
        }

        async fn create_dir(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            Err(unsupported_test_operation())
        }

        async fn stat(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpStat,
        ) -> opendal::Result<RpStat> {
            Err(unsupported_test_operation())
        }

        fn read(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            Err(unsupported_test_operation())
        }

        fn write(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            Err(unsupported_test_operation())
        }

        fn delete(&self, _ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            Err(unsupported_test_operation())
        }

        fn list(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<Self::Lister> {
            Ok(CountingLister {
                pulls: Arc::clone(&self.pulls),
                next: 0,
            })
        }

        fn copy(
            &self,
            _ctx: &OperationContext,
            _from: &str,
            _to: &str,
            _args: OpCopy,
            _opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            Err(unsupported_test_operation())
        }

        async fn rename(
            &self,
            _ctx: &OperationContext,
            _from: &str,
            _to: &str,
            _args: OpRename,
        ) -> opendal::Result<RpRename> {
            Err(unsupported_test_operation())
        }

        async fn presign(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            Err(unsupported_test_operation())
        }
    }

    fn unsupported_test_operation() -> opendal::Error {
        opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "operation is not supported by the test service",
        )
    }

    struct CountingLister {
        pulls: Arc<AtomicUsize>,
        next: usize,
    }

    impl oio::List for CountingLister {
        async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
            self.pulls.fetch_add(1, AtomicOrdering::SeqCst);
            if self.next == 0 {
                self.next += 1;
                return Ok(Some(oio::Entry::new(
                    "objects/first.txt",
                    Metadata::new(EntryMode::FILE).with_content_length(1),
                )));
            }

            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "limited listing polled past the requested row",
            ))
        }
    }

    fn setup_memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn setup_fs_file_io() -> FileIO {
        FileIOBuilder::new("file").build().unwrap()
    }

    fn local_file_path(path: &std::path::Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized.starts_with('/') {
            format!("file:{normalized}")
        } else {
            format!("file:/{normalized}")
        }
    }

    async fn common_test_get_status(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let status = file_io.get_status(path).await.unwrap();
        assert_eq!(status.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_delete_file(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        file_io.delete_file(path).await.unwrap();

        let exists = file_io.exists(path).await.unwrap();
        assert!(!exists);
    }

    async fn common_test_mkdirs(file_io: &FileIO, dir_path: &str) {
        file_io.mkdirs(dir_path).await.unwrap();

        let exists = file_io.exists(dir_path).await.unwrap();
        assert!(exists);

        let _ = fs::remove_dir_all(dir_path.strip_prefix("file:/").unwrap());
    }

    async fn common_test_rename(file_io: &FileIO, src: &str, dst: &str) {
        let output = file_io.new_output(src).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        file_io.rename(src, dst).await.unwrap();

        let exists_old = file_io.exists(src).await.unwrap();
        let exists_new = file_io.exists(dst).await.unwrap();
        assert!(!exists_old);
        assert!(exists_new);

        file_io.delete_file(dst).await.unwrap();
    }

    async fn common_test_list_status_paths(file_io: &FileIO, dir_path: &str) {
        if let Some(local_dir) = dir_path.strip_prefix("file:/") {
            let _ = fs::remove_dir_all(local_dir);
        }

        file_io.mkdirs(dir_path).await.unwrap();

        let file_a = format!("{dir_path}a.txt");
        let file_b = format!("{dir_path}b.txt");
        for file in [&file_a, &file_b] {
            file_io
                .new_output(file)
                .unwrap()
                .write(Bytes::from("test data"))
                .await
                .unwrap();
        }

        let statuses = file_io.list_status(dir_path).await.unwrap();
        assert_eq!(statuses.len(), 2);

        let expected_paths: BTreeSet<String> =
            [file_a.clone(), file_b.clone()].into_iter().collect();
        let actual_paths: BTreeSet<String> =
            statuses.iter().map(|status| status.path.clone()).collect();
        assert_eq!(
            actual_paths, expected_paths,
            "list_status should return exact entry paths"
        );

        file_io.delete_dir(dir_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_recursive_listing_stops_after_limit() {
        let pulls = Arc::new(AtomicUsize::new(0));
        let file_io = setup_memory_file_io().with_provider(Arc::new(CountingListProvider {
            pulls: Arc::clone(&pulls),
        }));

        let statuses = file_io
            .list_status_recursive_with_limit("counting:/objects/", Some(1))
            .await
            .unwrap();

        assert_eq!(statuses.len(), 1);
        assert_eq!(pulls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_recursive_listing_stream_yields_before_polling_next_entry() {
        let pulls = Arc::new(AtomicUsize::new(0));
        let file_io = setup_memory_file_io().with_provider(Arc::new(CountingListProvider {
            pulls: Arc::clone(&pulls),
        }));

        let mut statuses = file_io
            .list_status_recursive_stream("counting:/objects/", None)
            .await
            .unwrap();
        let first = statuses.try_next().await.unwrap().unwrap();

        assert!(first.path.ends_with("first.txt"));
        assert_eq!(pulls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_delete_file_memory() {
        let file_io = setup_memory_file_io();
        common_test_delete_file(&file_io, "memory:/test_file_delete_mem").await;
    }

    #[tokio::test]
    async fn test_empty_path_should_return_error_for_exists_fs() {
        let file_io = setup_fs_file_io();
        let result = file_io.exists("").await;
        assert!(matches!(result, Err(Error::ConfigInvalid { .. })));
    }

    #[tokio::test]
    async fn test_empty_path_should_return_error_for_exists_memory() {
        let file_io = setup_memory_file_io();
        let result = file_io.exists("").await;
        assert!(matches!(result, Err(Error::ConfigInvalid { .. })));
    }

    #[tokio::test]
    async fn test_exists_dir_memory() {
        let file_io = setup_memory_file_io();

        file_io.mkdirs("memory:/empty").await.unwrap();
        assert!(file_io.exists_dir("memory:/empty").await.unwrap());

        file_io
            .new_output("memory:/markerless/child")
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();
        assert!(file_io.exists_dir("memory:/markerless").await.unwrap());

        assert!(!file_io.exists_dir("memory:/missing").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_operator_reuse_across_file_io_calls() {
        let file_io = setup_memory_file_io();
        let path = "memory:/tmp/reuse_case";
        let dir = "memory:/tmp/";

        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();

        assert!(file_io.exists(path).await.unwrap());
        assert_eq!(file_io.get_status(path).await.unwrap().size, 4);
        assert!(file_io
            .list_status(dir)
            .await
            .unwrap()
            .iter()
            .any(|status| status.path == path));

        file_io.delete_dir(dir).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_operator_not_shared_between_file_io_instances() {
        let file_io_1 = setup_memory_file_io();
        let file_io_2 = setup_memory_file_io();
        let path = "memory:/tmp/reuse_isolation_case";

        file_io_1
            .new_output(path)
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();

        assert!(file_io_1.exists(path).await.unwrap());
        assert!(!file_io_2.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_status_fs() {
        let file_io = setup_fs_file_io();
        common_test_get_status(&file_io, "file:/tmp/test_file_get_status_fs").await;
    }

    #[tokio::test]
    async fn test_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_exists(&file_io, "file:/tmp/test_file_exists_fs").await;
    }

    #[tokio::test]
    async fn test_delete_file_fs() {
        let file_io = setup_fs_file_io();
        common_test_delete_file(&file_io, "file:/tmp/test_file_delete_fs").await;
    }

    #[tokio::test]
    async fn test_mkdirs_fs() {
        let file_io = setup_fs_file_io();
        common_test_mkdirs(&file_io, "file:/tmp/test_fs_dir/").await;
    }

    #[tokio::test]
    async fn test_rename_fs() {
        let file_io = setup_fs_file_io();
        common_test_rename(
            &file_io,
            "file:/tmp/test_file_fs_z",
            "file:/tmp/new_test_file_fs_o",
        )
        .await;
    }

    #[tokio::test]
    async fn test_list_status_fs_should_return_entry_paths() {
        let file_io = setup_fs_file_io();
        common_test_list_status_paths(&file_io, "file:/tmp/test_list_status_paths_fs/").await;
    }

    #[test]
    fn test_from_path_detects_local_fs_path() {
        let dir = tempdir().unwrap();
        let file_io = FileIO::from_path(dir.path().to_string_lossy())
            .unwrap()
            .build()
            .unwrap();
        let path = local_file_path(&dir.path().join("from_path_detects_local_fs_path.txt"));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from("data"))
                .await
                .unwrap();
            assert!(file_io.exists(&path).await.unwrap());
        });
    }
}

#[cfg(all(
    test,
    any(
        feature = "storage-cos",
        feature = "storage-obs",
        feature = "storage-gcs",
        feature = "storage-azdls"
    )
))]
mod object_storage_path_test {
    use super::*;

    fn assert_relative_paths(file_io: &FileIO, path: &str, expected_relative_path: &str) {
        let input = file_io.new_input(path).unwrap();
        assert_eq!(input.location(), path);
        let FileSource::Static { relative_path, .. } = input.source else {
            panic!("expected static input")
        };
        assert_eq!(relative_path, expected_relative_path);

        let output = file_io.new_output(path).unwrap();
        assert_eq!(output.location(), path);
        let FileSource::Static { relative_path, .. } = output.source else {
            panic!("expected static output")
        };
        assert_eq!(relative_path, expected_relative_path);

        let (_op, relative_path) = file_io.create_static(path).unwrap();
        assert_eq!(relative_path, expected_relative_path);

        let base_path = &path[..path.len() - relative_path.len()];
        assert_eq!(format!("{base_path}{relative_path}"), path);
    }

    #[cfg(feature = "storage-azdls")]
    #[test]
    fn test_azdls_root_status_path_without_trailing_slash() {
        assert_eq!(
            status_path(
                "abfs://filesystem@account.dfs.core.windows.net",
                "warehouse/"
            ),
            "abfs://filesystem@account.dfs.core.windows.net/warehouse/"
        );
        assert_eq!(
            status_path(
                "abfs://filesystem@account.dfs.core.windows.net/",
                "warehouse/"
            ),
            "abfs://filesystem@account.dfs.core.windows.net/warehouse/"
        );
    }

    #[cfg(feature = "storage-cos")]
    #[test]
    fn test_cos_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["cosn", "cos"] {
            let path = format!("{scheme}://bucket/warehouse/table/data.parquet");
            let dir_path = format!("{scheme}://bucket/warehouse/table/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_props([
                    ("fs.cosn.endpoint", "https://cos.ap-shanghai.myqcloud.com"),
                    ("fs.cosn.userinfo.secretId", "secret-id"),
                    ("fs.cosn.userinfo.secretKey", "secret-key"),
                    ("fs.cosn.disable-config-load", "true"),
                ])
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/table/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/table/");
        }
    }

    #[cfg(feature = "storage-obs")]
    #[test]
    fn test_obs_file_io_relative_paths() {
        let file_io = FileIO::from_path("obs://bucket/warehouse")
            .unwrap()
            .with_props([
                (
                    "fs.obs.endpoint",
                    "https://obs.cn-north-4.myhuaweicloud.com",
                ),
                ("fs.obs.access.key", "access-key"),
                ("fs.obs.secret.key", "secret-key"),
            ])
            .build()
            .unwrap();

        assert_relative_paths(
            &file_io,
            "obs://bucket/warehouse/table/data.parquet",
            "warehouse/table/data.parquet",
        );
        assert_relative_paths(
            &file_io,
            "obs://bucket/warehouse/table/",
            "warehouse/table/",
        );
    }

    #[cfg(feature = "storage-gcs")]
    #[test]
    fn test_gcs_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["gs", "gcs"] {
            let path = format!("{scheme}://bucket/warehouse/table/data.parquet");
            let dir_path = format!("{scheme}://bucket/warehouse/table/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_props([
                    ("gcs.allow-anonymous", "true"),
                    ("gcs.disable-config-load", "true"),
                    ("gcs.disable-vm-metadata", "true"),
                ])
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/table/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/table/");
        }
    }

    #[cfg(feature = "storage-azdls")]
    #[test]
    fn test_azdls_file_io_relative_paths_and_scheme_aliases() {
        for scheme in ["abfs", "abfss"] {
            let path = format!(
                "{scheme}://filesystem@account.dfs.core.windows.net/warehouse/data.parquet"
            );
            let dir_path = format!("{scheme}://filesystem@account.dfs.core.windows.net/warehouse/");
            let file_io = FileIO::from_path(&path)
                .unwrap()
                .with_prop("azure.account-key", "account-key")
                .build()
                .unwrap();

            assert_relative_paths(&file_io, &path, "warehouse/data.parquet");
            assert_relative_paths(&file_io, &dir_path, "warehouse/");
        }
    }
}

#[cfg(test)]
mod input_output_test {
    use std::sync::Arc;

    use super::*;
    use crate::common::{CatalogOptions, Options};
    use crate::io::cache::{LocalCache, LocalCacheConfig};
    use bytes::Bytes;

    fn setup_memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn setup_fs_file_io() -> FileIO {
        FileIOBuilder::new("file").build().unwrap()
    }

    fn setup_cached_fs_file_io(cache_directory: &std::path::Path) -> FileIO {
        let mut options = Options::new();
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(
            CatalogOptions::LOCAL_CACHE_DIR,
            cache_directory.to_string_lossy(),
        );
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
        let cache = Arc::new(
            LocalCache::new(LocalCacheConfig::from_options(&options).unwrap().unwrap()).unwrap(),
        );
        FileIOBuilder::new("file")
            .with_local_cache(cache)
            .build()
            .unwrap()
    }

    async fn common_test_output_file_write_and_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let content = input.read().await.unwrap();

        assert_eq!(&content[..], b"hello world");

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_output_file_exists(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let exists = output.exists().await.unwrap();
        assert!(exists);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_metadata(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let metadata = input.metadata().await.unwrap();

        assert_eq!(metadata.size, 11);

        file_io.delete_file(path).await.unwrap();
    }

    async fn common_test_input_file_partial_read(file_io: &FileIO, path: &str) {
        let output = file_io.new_output(path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from("hello world")).await.unwrap();
        writer.close().await.unwrap();

        let input = output.to_input_file();
        let reader = input.reader().await.unwrap();
        let partial_content = reader.read(0..5).await.unwrap(); // read "hello"

        assert_eq!(&partial_content[..], b"hello");

        file_io.delete_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_write_and_read(&file_io, "memory:/test_file_rw_mem").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_memory() {
        let file_io = setup_memory_file_io();
        common_test_output_file_exists(&file_io, "memory:/test_file_exist_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_metadata(&file_io, "memory:/test_file_meta_mem").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_memory() {
        let file_io = setup_memory_file_io();
        common_test_input_file_partial_read(&file_io, "memory:/test_file_part_read_mem").await;
    }

    #[tokio::test]
    async fn test_file_read_cache_key_is_scoped_to_file_io_context() {
        let path = "memory:/cache-key.parquet";
        let first = setup_memory_file_io();
        first
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"first"))
            .await
            .unwrap();
        let first_key = first
            .new_input(path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();
        let clone_key = first
            .clone()
            .new_input(path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();

        let second = setup_memory_file_io();
        second
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"second"))
            .await
            .unwrap();
        let second_key = second
            .new_input(path)
            .unwrap()
            .reader()
            .await
            .unwrap()
            .cache_key()
            .unwrap()
            .to_string();

        assert_eq!(first_key, clone_key);
        assert_ne!(first_key, second_key);
    }

    #[tokio::test]
    async fn test_file_format_metadata_cache_size_reaches_file_reader() {
        let path = "memory:/metadata-cache-size.parquet";
        let default = setup_memory_file_io();
        default
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert_eq!(
            default
                .new_input(path)
                .unwrap()
                .reader()
                .await
                .unwrap()
                .file_format_metadata_cache_max_bytes(),
            DEFAULT_FILE_FORMAT_METADATA_CACHE_MAX_BYTES
        );

        let configured = FileIOBuilder::new("memory")
            .with_prop(CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE, "1 mb")
            .build()
            .unwrap();
        configured
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert_eq!(
            configured
                .new_input(path)
                .unwrap()
                .reader()
                .await
                .unwrap()
                .file_format_metadata_cache_max_bytes(),
            1024 * 1024
        );
    }

    #[test]
    fn test_file_format_metadata_cache_size_rejects_invalid_value() {
        let error = FileIOBuilder::new("memory")
            .with_prop(
                CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE,
                "invalid",
            )
            .build()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains(CatalogOptions::FILE_FORMAT_METADATA_CACHE_MAX_SIZE));
    }

    #[tokio::test]
    async fn test_output_file_write_and_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_write_and_read(&file_io, "file:/tmp/test_file_fs_rw").await;
    }

    #[tokio::test]
    async fn test_output_file_exists_fs() {
        let file_io = setup_fs_file_io();
        common_test_output_file_exists(&file_io, "file:/tmp/test_file_exists").await;
    }

    #[tokio::test]
    async fn test_input_file_metadata_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_metadata(&file_io, "file:/tmp/test_file_meta").await;
    }

    #[tokio::test]
    async fn test_input_file_partial_read_fs() {
        let file_io = setup_fs_file_io();
        common_test_input_file_partial_read(&file_io, "file:/tmp/test_file_read_fs").await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_serves_full_read_after_source_disappears() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"cached metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"cached metadata")
        );
        std::fs::remove_file(&source_path).unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"cached metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_serves_range_after_source_disappears() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"cached metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        let reader = file_io
            .new_input(&location)
            .unwrap()
            .reader()
            .await
            .unwrap();
        assert_eq!(
            reader.read(1..7).await.unwrap(),
            Bytes::from_static(b"ached ")
        );
        std::fs::remove_file(&source_path).unwrap();
        let reader = file_io
            .new_input(&location)
            .unwrap()
            .reader()
            .await
            .unwrap();
        assert_eq!(
            reader.read(1..7).await.unwrap(),
            Bytes::from_static(b"ached ")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_successful_write() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io
            .new_output(&location)
            .unwrap()
            .write(Bytes::from_static(b"new metadata"))
            .await
            .unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_equivalent_local_path_alias() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let file_location = format!("file:{}", source_path.display());
        let absolute_location = source_path.to_string_lossy();
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&file_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io
            .new_output(absolute_location.as_ref())
            .unwrap()
            .write(Bytes::from_static(b"new metadata"))
            .await
            .unwrap();

        assert_eq!(
            file_io
                .new_input(&file_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_delete() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io.delete_file(&location).await.unwrap();
        std::fs::write(&source_path, b"new metadata").unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_delete_directory() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let snapshot_directory = source_directory.path().join("snapshot");
        std::fs::create_dir(&snapshot_directory).unwrap();
        let source_path = snapshot_directory.join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let directory_location = format!("file:{}", snapshot_directory.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        file_io.delete_dir(&directory_location).await.unwrap();
        std::fs::create_dir(&snapshot_directory).unwrap();
        std::fs::write(&source_path, b"new metadata").unwrap();
        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_copy_target() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        let target_path = source_directory.path().join("snapshot-2");
        std::fs::write(&source_path, b"source value").unwrap();
        std::fs::write(&target_path, b"stale target").unwrap();
        let source_location = format!("file:{}", source_path.display());
        let target_location = format!("file:{}", target_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"stale target")
        );
        file_io
            .copy_file(&source_location, &target_location)
            .await
            .unwrap();

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
    }

    #[tokio::test]
    async fn test_streaming_copy_crosses_chunk_boundary() {
        let file_io = setup_memory_file_io();
        let source = "memory:/format-copy/source.parquet";
        let target = "memory:/format-copy/target.parquet";
        let mut payload = vec![0u8; 8 * 1024 * 1024 + 17];
        payload[0] = 3;
        payload[8 * 1024 * 1024 - 1] = 7;
        payload[8 * 1024 * 1024] = 11;
        payload[8 * 1024 * 1024 + 16] = 13;
        file_io
            .new_output(source)
            .unwrap()
            .write(Bytes::from(payload.clone()))
            .await
            .unwrap();

        file_io.copy_file_streaming(source, target).await.unwrap();
        assert_eq!(
            file_io.new_input(target).unwrap().read().await.unwrap(),
            Bytes::from(payload)
        );
        assert!(file_io.exists(source).await.unwrap());
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_source_and_target_after_rename() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        let target_path = source_directory.path().join("snapshot-2");
        std::fs::write(&source_path, b"source value").unwrap();
        std::fs::write(&target_path, b"target value").unwrap();
        let source_location = format!("file:{}", source_path.display());
        let target_location = format!("file:{}", target_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&source_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"target value")
        );
        file_io
            .rename(&source_location, &target_location)
            .await
            .unwrap();
        std::fs::write(&source_path, b"new source!!").unwrap();

        assert_eq!(
            file_io
                .new_input(&target_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"source value")
        );
        assert_eq!(
            file_io
                .new_input(&source_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new source!!")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_directories_after_rename() {
        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let old_directory = source_directory.path().join("old");
        let new_directory = source_directory.path().join("new");
        std::fs::create_dir(&old_directory).unwrap();
        std::fs::create_dir(&new_directory).unwrap();
        let old_snapshot = old_directory.join("snapshot-1");
        let new_snapshot = new_directory.join("snapshot-1");
        std::fs::write(&old_snapshot, b"old directory").unwrap();
        std::fs::write(&new_snapshot, b"new directory").unwrap();
        let old_directory_location = format!("file:{}", old_directory.display());
        let new_directory_location = format!("file:{}", new_directory.display());
        let old_snapshot_location = format!("file:{}", old_snapshot.display());
        let new_snapshot_location = format!("file:{}", new_snapshot.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io
                .new_input(&old_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old directory")
        );
        assert_eq!(
            file_io
                .new_input(&new_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"new directory")
        );
        std::fs::remove_dir_all(&new_directory).unwrap();
        file_io
            .rename(&old_directory_location, &new_directory_location)
            .await
            .unwrap();
        std::fs::create_dir(&old_directory).unwrap();
        std::fs::write(&old_snapshot, b"replacement!!").unwrap();

        assert_eq!(
            file_io
                .new_input(&new_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"old directory")
        );
        assert_eq!(
            file_io
                .new_input(&old_snapshot_location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"replacement!!")
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_file_io_local_cache_invalidates_after_streaming_write_shutdown() {
        use tokio::io::AsyncWriteExt;

        let source_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let source_path = source_directory.path().join("snapshot-1");
        std::fs::write(&source_path, b"old metadata").unwrap();
        let location = format!("file:{}", source_path.display());
        let file_io = setup_cached_fs_file_io(cache_directory.path());

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"old metadata")
        );
        let mut writer = file_io
            .new_output(&location)
            .unwrap()
            .async_writer()
            .await
            .unwrap();
        writer.write_all(b"new metadata").await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(
            file_io.new_input(&location).unwrap().read().await.unwrap(),
            Bytes::from_static(b"new metadata")
        );
    }
}
