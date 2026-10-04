// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use libloading::Library;
use opendal::raw::*;
use opendal::{Buffer, Builder, BytesRange, Capability, EntryMode, Error, ErrorKind, Metadata};
use opendal::{OperationContext, Operator, Result};
use tokio::sync::{OnceCell, Semaphore};

const PREFIX: &str = "fs.oss.cpp.";
const DEFAULT_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct OssCppStorageConfig {
    library: String,
    strings: Vec<CString>,
    concurrency: usize,
    gate: Arc<Semaphore>,
    connect_timeout: i64,
    request_timeout: i64,
    retry_attempts: i64,
    path_style: u8,
}

impl Default for OssCppStorageConfig {
    fn default() -> Self {
        Self {
            library: String::new(),
            strings: Vec::new(),
            concurrency: 0,
            gate: Arc::new(Semaphore::new(0)),
            connect_timeout: 0,
            request_timeout: 0,
            retry_attempts: 0,
            path_style: 0,
        }
    }
}

impl Debug for OssCppStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssCppStorageConfig")
            .field("library", &self.library)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

pub(crate) fn oss_cpp_config_parse(
    props: HashMap<String, String>,
) -> crate::Result<OssCppStorageConfig> {
    let parse = || -> Result<OssCppStorageConfig> {
        let required = |key: &str| -> Result<String> {
            props
                .get(key)
                .filter(|s| !s.is_empty())
                .cloned()
                .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, format!("Missing {key}")))
        };
        let number = |name: &str, default: i64| -> Result<i64> {
            let key = format!("{PREFIX}{name}");
            match props.get(&key) {
                None => Ok(default),
                Some(value) => value.parse::<i64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                    Error::new(ErrorKind::ConfigInvalid, format!("{key} must be positive"))
                }),
            }
        };
        let endpoint = required("fs.oss.endpoint")?;
        let endpoint = if endpoint.contains("://") {
            endpoint
        } else {
            format!("https://{endpoint}")
        };
        let url = url::Url::parse(&endpoint)
            .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "Invalid OSS endpoint"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "OSS endpoint must be an HTTP(S) origin",
            ));
        }
        let strings = [
            endpoint,
            required("fs.oss.region")?,
            required("fs.oss.accessKeyId")?,
            required("fs.oss.accessKeySecret")?,
            props
                .get("fs.oss.securityToken")
                .cloned()
                .unwrap_or_default(),
            // The native SDK appends this to its own User-Agent.
            format!("{} oss-cpp", super::user_agent::oss_user_agent(&props)),
        ]
        .into_iter()
        .map(|s| cstring(&s))
        .collect::<Result<Vec<_>>>()?;
        let concurrency = number("max.concurrent.requests", DEFAULT_CONCURRENCY as i64)? as usize;
        if concurrency > Semaphore::MAX_PERMITS {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "OSS C++ concurrency exceeds semaphore limit",
            ));
        }
        let path_style = match props.get("fs.oss.cpp.path-style").map(String::as_str) {
            None | Some("false") => 0,
            Some("true") => 1,
            _ => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "Invalid OSS C++ path-style",
                ))
            }
        };
        Ok(OssCppStorageConfig {
            library: required("fs.oss.cpp.library.path")?,
            strings,
            concurrency,
            gate: Arc::new(Semaphore::new(concurrency)),
            connect_timeout: number("connect.timeout-ms", 10_000)?,
            request_timeout: number("request.timeout-ms", 30_000)?,
            retry_attempts: number("retry.max-attempts", 3)?,
            path_style,
        })
    };
    parse().map_err(|e| crate::Error::ConfigInvalid {
        message: e.to_string(),
    })
}

pub(crate) fn oss_cpp_config_build(
    config: &OssCppStorageConfig,
    bucket: &str,
) -> crate::Result<Operator> {
    Operator::new(CppBuilder {
        config: config.clone(),
        bucket: bucket.to_string(),
    })
    .map_err(|e| crate::Error::IoUnexpected {
        message: "Cannot build OSS C++ operator".to_string(),
        source: Box::new(e),
    })
}

#[derive(Default)]
struct CppBuilder {
    config: OssCppStorageConfig,
    bucket: String,
}

impl Builder for CppBuilder {
    type Config = ();
    fn build(self) -> Result<impl Service> {
        if self.config.strings.len() != 6 || self.config.concurrency == 0 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "Missing OSS C++ configuration",
            ));
        }
        Ok(CppService {
            info: ServiceInfo::new("oss-cpp", "/", &self.bucket),
            client: Arc::new(LazyClient {
                gate: self.config.gate.clone(),
                config: self.config,
                bucket: self.bucket,
                client: OnceCell::new(),
            }),
        })
    }
}

struct CppService {
    info: ServiceInfo,
    client: Arc<LazyClient>,
}

impl Debug for CppService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssCppService")
            .field("info", &self.info)
            .finish()
    }
}

impl Service for CppService {
    type Reader = CppReader;
    type Writer = ();
    type Lister = CppLister;
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }
    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            read_with_suffix: true,
            list: true,
            list_with_recursive: true,
            shared: true,
            ..Default::default()
        }
    }
    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let path = path.to_string();
        let metadata = self.client.call(move |c| c.stat(&path)).await?;
        Ok(RpStat::new(metadata))
    }
    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(CppReader {
            client: self.client.clone(),
            path: path.to_string(),
        })
    }
    fn list(&self, _: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(CppLister {
            client: self.client.clone(),
            path: path.to_string(),
            recursive: args.recursive(),
            max_keys: args.limit().unwrap_or(1000).clamp(1, 1000) as u32,
            token: String::new(),
            done: false,
            entries: Vec::new().into_iter(),
        })
    }
    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unsupported()
    }
    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unsupported()
    }
    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unsupported()
    }
    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpCopy,
        _: OpCopier,
    ) -> Result<Self::Copier> {
        unsupported()
    }
    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unsupported()
    }
    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unsupported()
    }
}

fn unsupported<T>() -> Result<T> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "OSS C++ backend is read-only",
    ))
}

struct CppReader {
    client: Arc<LazyClient>,
    path: String,
}

impl oio::Read for CppReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (_, buffer) = self.read(range).await?;
        Ok((RpRead::default(), Box::new(buffer)))
    }
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let path = self.path.clone();
        let data = self.client.call(move |c| c.read(&path, range)).await?;
        Ok((RpRead::default(), Buffer::from(data)))
    }
}

struct CppLister {
    client: Arc<LazyClient>,
    path: String,
    recursive: bool,
    max_keys: u32,
    token: String,
    done: bool,
    entries: std::vec::IntoIter<oio::Entry>,
}

impl oio::List for CppLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Some(entry) = self.entries.next() {
                return Ok(Some(entry));
            }
            if self.done {
                return Ok(None);
            }
            let path = self.path.clone();
            let token = self.token.clone();
            let recursive = self.recursive;
            let max_keys = self.max_keys;
            let (entries, next) = self
                .client
                .call(move |c| c.list(&path, &token, recursive, max_keys))
                .await?;
            self.done = next.is_empty();
            self.token = next;
            self.entries = entries.into_iter();
        }
    }
}

struct LazyClient {
    config: OssCppStorageConfig,
    bucket: String,
    client: OnceCell<Arc<Client>>,
    gate: Arc<Semaphore>,
}

// Native clients/connection pools cannot be inherited across fork. Check before
// touching OnceCell or Semaphore: either may have been locked by a parent thread.
fn ensure_process() -> Result<()> {
    static PID: AtomicU32 = AtomicU32::new(0);
    let current = std::process::id();
    match PID.compare_exchange(0, current, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(()),
        Err(pid) if pid == current => Ok(()),
        Err(_) => Err(Error::new(
            ErrorKind::Unsupported,
            "OSS C++ SDK cannot be used after fork; use spawn workers",
        )),
    }
}

impl LazyClient {
    async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Client) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        ensure_process()?;
        let client = self
            .client
            .get_or_try_init(|| async {
                let config = self.config.clone();
                let bucket = self.bucket.clone();
                blocking(move || Client::new(&config, &bucket).map(Arc::new)).await
            })
            .await?
            .clone();
        let permit = self
            .gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "OSS C++ request gate closed"))?;
        blocking(move || {
            // Cancellation does not stop a synchronous C++ request. Keep both
            // the client and permit alive until the actual native call returns.
            let _permit = permit;
            operation(&client)
        })
        .await
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| Error::new(ErrorKind::Unexpected, "OSS C++ blocking task failed"))?
}

#[repr(C)]
struct NativeConfig {
    endpoint: *const c_char,
    region: *const c_char,
    access_key: *const c_char,
    secret_key: *const c_char,
    token: *const c_char,
    user_agent: *const c_char,
    connect_timeout_ms: i64,
    request_timeout_ms: i64,
    retry_attempts: i64,
    path_style: u8,
}

#[repr(C)]
struct NativeError {
    status: i32,
    code: [c_char; 128],
    request_id: [c_char; 256],
}
impl Default for NativeError {
    fn default() -> Self {
        Self {
            status: 0,
            code: [0; 128],
            request_id: [0; 256],
        }
    }
}
impl NativeError {
    fn check(&self, status: i32) -> Result<()> {
        if status == 0 {
            return Ok(());
        }
        let kind = match self.status {
            404 => ErrorKind::NotFound,
            401 | 403 => ErrorKind::PermissionDenied,
            429 | 503 => ErrorKind::RateLimited,
            416 => ErrorKind::RangeNotSatisfied,
            _ => ErrorKind::Unexpected,
        };
        // Only expose status, symbolic code and request ID, never request URLs,
        // authorization headers or SDK exception text.
        Err(Error::new(kind, "OSS C++ operation failed")
            .with_context("status", self.status.to_string())
            .with_context("code", native_string(self.code.as_ptr()))
            .with_context("request_id", native_string(self.request_id.as_ptr())))
    }
}

#[repr(C)]
struct NativeMetadata {
    size: i64,
    modified: [c_char; 64],
}
type EntryCallback =
    unsafe extern "C" fn(*mut c_void, *const c_char, usize, i64, *const c_char, u8);
type Create = unsafe extern "C" fn(*const NativeConfig, *mut NativeError) -> *mut c_void;
type Destroy = unsafe extern "C" fn(*mut c_void);
type Stat = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    *mut NativeMetadata,
    *mut NativeError,
) -> i32;
type Read = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    u64,
    usize,
    *mut u8,
    *mut NativeError,
) -> i32;
type List = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    *const c_char,
    u8,
    u32,
    EntryCallback,
    *mut c_void,
    *mut *mut c_char,
    *mut NativeError,
) -> i32;
type FreeString = unsafe extern "C" fn(*mut c_char);

struct Api {
    create: Create,
    destroy: Destroy,
    stat: Stat,
    read: Read,
    list: List,
    free_string: FreeString,
    _library: Library,
}
impl Api {
    fn load(path: &str) -> Result<Self> {
        // SAFETY: loading native code is explicit opt-in via a trusted library
        // path. Symbols follow bridge.h; retain the library for their lifetime.
        unsafe {
            let library = Library::new(path).map_err(|_| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "Cannot load fs.oss.cpp.library.path or its dependencies",
                )
            })?;
            let load_error = || Error::new(ErrorKind::ConfigInvalid, "Invalid OSS C++ bridge ABI");
            let version = library
                .get::<unsafe extern "C" fn() -> u32>(b"oss_cpp_bridge_abi_version\0")
                .map_err(|_| load_error())?;
            if version() != 2 {
                return Err(load_error());
            }
            Ok(Self {
                create: *library
                    .get(b"oss_cpp_bridge_create\0")
                    .map_err(|_| load_error())?,
                destroy: *library
                    .get(b"oss_cpp_bridge_destroy\0")
                    .map_err(|_| load_error())?,
                stat: *library
                    .get(b"oss_cpp_bridge_stat\0")
                    .map_err(|_| load_error())?,
                read: *library
                    .get(b"oss_cpp_bridge_read\0")
                    .map_err(|_| load_error())?,
                list: *library
                    .get(b"oss_cpp_bridge_list\0")
                    .map_err(|_| load_error())?,
                free_string: *library
                    .get(b"oss_cpp_bridge_free_string\0")
                    .map_err(|_| load_error())?,
                _library: library,
            })
        }
    }
}

struct Client {
    api: Arc<Api>,
    pointer: *mut c_void,
    bucket: CString,
    pid: u32,
}
// SAFETY: the bridge uses a concurrent OSSClient and per-call outputs. The
// immutable client is released only after all blocking calls drop their Arc.
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    fn new(config: &OssCppStorageConfig, bucket: &str) -> Result<Self> {
        let bucket = cstring(bucket)?;
        let api = Arc::new(Api::load(&config.library)?);
        let s = &config.strings;
        let input = NativeConfig {
            endpoint: s[0].as_ptr(),
            region: s[1].as_ptr(),
            access_key: s[2].as_ptr(),
            secret_key: s[3].as_ptr(),
            token: s[4].as_ptr(),
            user_agent: s[5].as_ptr(),
            connect_timeout_ms: config.connect_timeout,
            request_timeout_ms: config.request_timeout,
            retry_attempts: config.retry_attempts,
            path_style: config.path_style,
        };
        let mut error = NativeError::default();
        let pointer = unsafe { (api.create)(&input, &mut error) };
        if pointer.is_null() {
            error.check(-1)?;
            unreachable!();
        }
        Ok(Self {
            api,
            pointer,
            bucket,
            pid: std::process::id(),
        })
    }
    fn stat(&self, key: &str) -> Result<Metadata> {
        if key.is_empty() {
            return Ok(Metadata::new(EntryMode::DIR));
        }
        if key.ends_with('/') {
            let (entries, _) = self.list(key, "", true, 1)?;
            return if entries.is_empty() {
                Err(Error::new(ErrorKind::NotFound, "OSS directory not found"))
            } else {
                Ok(Metadata::new(EntryMode::DIR))
            };
        }
        let key = cstring(key)?;
        let mut out = NativeMetadata {
            size: 0,
            modified: [0; 64],
        };
        let mut error = NativeError::default();
        let status = unsafe {
            (self.api.stat)(
                self.pointer,
                self.bucket.as_ptr(),
                key.as_ptr(),
                &mut out,
                &mut error,
            )
        };
        error.check(status)?;
        metadata(out.size, &native_string(out.modified.as_ptr()), false)
    }
    fn read(&self, key: &str, range: BytesRange) -> Result<Vec<u8>> {
        let (offset, length) = match range {
            BytesRange::Range {
                offset,
                size: Some(length),
            } => (offset, length),
            BytesRange::Range { offset, size: None } => {
                let size = self.stat(key)?.content_length();
                let length = size.checked_sub(offset).ok_or_else(|| {
                    Error::new(ErrorKind::RangeNotSatisfied, "Range starts after EOF")
                })?;
                (offset, length)
            }
            BytesRange::Suffix { size } => {
                let length = self.stat(key)?.content_length();
                (length.saturating_sub(size), size.min(length))
            }
        };
        offset
            .checked_add(length)
            .ok_or_else(|| Error::new(ErrorKind::RangeNotSatisfied, "Range overflow"))?;
        let length = usize::try_from(length)
            .map_err(|_| Error::new(ErrorKind::RangeNotSatisfied, "Range too large"))?;
        if length == 0 {
            return Ok(Vec::new());
        }
        if length > i64::MAX as usize {
            return Err(Error::new(ErrorKind::RangeNotSatisfied, "Range too large"));
        }
        let key = cstring(key)?;
        let mut data = Vec::new();
        data.try_reserve_exact(length)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Cannot allocate read buffer"))?;
        data.resize(length, 0);
        let mut error = NativeError::default();
        let status = unsafe {
            (self.api.read)(
                self.pointer,
                self.bucket.as_ptr(),
                key.as_ptr(),
                offset,
                length,
                data.as_mut_ptr(),
                &mut error,
            )
        };
        error.check(status)?;
        Ok(data)
    }
    fn list(
        &self,
        prefix: &str,
        token: &str,
        recursive: bool,
        max_keys: u32,
    ) -> Result<(Vec<oio::Entry>, String)> {
        let prefix = cstring(prefix)?;
        let token = cstring(token)?;
        let mut entries = EntryCollector {
            entries: Vec::new(),
            error: None,
        };
        let mut next = std::ptr::null_mut();
        let mut error = NativeError::default();
        let status = unsafe {
            (self.api.list)(
                self.pointer,
                self.bucket.as_ptr(),
                prefix.as_ptr(),
                token.as_ptr(),
                u8::from(recursive),
                max_keys,
                collect_entry,
                &mut entries as *mut EntryCollector as *mut c_void,
                &mut next,
                &mut error,
            )
        };
        let next_token = if next.is_null() {
            String::new()
        } else {
            let value = native_string(next);
            unsafe { (self.api.free_string)(next) };
            value
        };
        error.check(status)?;
        if let Some(error) = entries.error {
            return Err(error);
        }
        Ok((entries.entries, next_token))
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            // Do not destroy native handles or unload their library in a forked child.
            std::mem::forget(self.api.clone());
            return;
        }
        unsafe { (self.api.destroy)(self.pointer) };
    }
}

struct EntryCollector {
    entries: Vec<oio::Entry>,
    error: Option<Error>,
}
unsafe extern "C" fn collect_entry(
    ctx: *mut c_void,
    key: *const c_char,
    len: usize,
    size: i64,
    modified: *const c_char,
    dir: u8,
) {
    // SAFETY: the synchronous bridge only invokes this callback while ctx and
    // the borrowed native strings are alive; no callback is retained.
    let collector = unsafe { &mut *(ctx as *mut EntryCollector) };
    if collector.error.is_some() {
        return;
    }
    let bytes = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), len) };
    let result = std::str::from_utf8(bytes)
        .map_err(|_| Error::new(ErrorKind::Unexpected, "Invalid UTF-8 object key"))
        .and_then(|path| {
            metadata(size, &native_string(modified), dir != 0)
                .map(|meta| oio::Entry::new(path, meta))
        });
    match result {
        Ok(entry) => collector.entries.push(entry),
        Err(error) => collector.error = Some(error),
    }
}
fn metadata(size: i64, modified: &str, dir: bool) -> Result<Metadata> {
    let size = u64::try_from(size)
        .map_err(|_| Error::new(ErrorKind::Unexpected, "Negative object size"))?;
    let mut meta = Metadata::new(if dir { EntryMode::DIR } else { EntryMode::FILE });
    meta.set_content_length(size);
    if !modified.is_empty() {
        let time = chrono::DateTime::parse_from_rfc2822(modified)
            .or_else(|_| chrono::DateTime::parse_from_rfc3339(modified))
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Invalid object modification time"))?;
        meta.set_last_modified(Timestamp::from_millisecond(time.timestamp_millis())?);
    }
    Ok(meta)
}
fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error::new(ErrorKind::ConfigInvalid, "NUL in OSS C++ input"))
}
fn native_string(value: *const c_char) -> String {
    // All native outputs are NUL terminated by bridge.cc.
    unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
#[path = "storage_oss_cpp_test.rs"]
mod tests;
